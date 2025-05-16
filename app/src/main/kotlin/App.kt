import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.http.ContentType
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import messaging.KeyDBClient
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SchemaUtils.createMissingTablesAndColumns
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import org.mindrot.jbcrypt.BCrypt
import org.slf4j.LoggerFactory

@Serializable
data class RegisterRequest(
    val email: String,
    val password: String,
    val name: String,
)

@Serializable data class LoginRequest(val email: String, val password: String)

@Serializable
data class AuthResponse(
    val token: String,
    val message: String,
    val correlationId: String = "",
)

@Serializable
data class RequestMessage(
    val type: String,
    val correlationId: String,
    val payload: String,
)

@Serializable
data class ResponseMessage(val correlationId: String, val payload: String)

@Serializable
data class UserProfile(
    val id: String,
    val email: String,
    val name: String,
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null,
    val createdAt: String,
)

@Serializable data class UpdateInfoRequest(val name: String)

@Serializable
data class UpdateMetricsRequest(
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null,
)

object Users : Table("users") {
    val id = uuid("id").clientDefault { UUID.randomUUID() }
    val email = varchar("email", 255).uniqueIndex()
    val passwordHash = varchar("password_hash", 255)
    val name = varchar("name", 255)
    val height = integer("height").nullable()
    val weight = integer("weight").nullable()
    val goal = varchar("goal", 50).nullable()
    val activityLevel = varchar("activity_level", 50).nullable()
    val createdAt = datetime("created_at").clientDefault { LocalDateTime.now() }
    override val primaryKey = PrimaryKey(id)
}

@Serializable
data class UserInfo(
    val id: String,
    val email: String,
    val name: String,
    val createdAt: String,
)

@Serializable
data class UserMetrics(
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null,
)

@Serializable
data class HistoryQueryParams(
    val from: String? = null,
    val to: String? = null,
    val field: String? = null,
)

@Serializable
data class HistoryRecord(
    val field: String,
    val oldValue: Int?,
    val newValue: Int?,
    val changedAt: String,
)

fun ResultRow.toProfile() =
    UserProfile(
        id = this[Users.id].toString(),
        email = this[Users.email],
        name = this[Users.name],
        height = this[Users.height],
        weight = this[Users.weight],
        goal = this[Users.goal],
        activityLevel = this[Users.activityLevel],
        createdAt = this[Users.createdAt].toString(),
    )

object UserMetricsHistory : Table("user_metrics_history") {
    val id = uuid("id").clientDefault { UUID.randomUUID() }
    val userId = uuid("user_id")
    val field = varchar("field", 10)
    val oldValue = integer("old_value").nullable()
    val newValue = integer("new_value").nullable()
    val changedAt = datetime("changed_at").clientDefault { LocalDateTime.now() }
}

inline fun <reified T> sendResponse(cid: String, body: T?, keydb: KeyDBClient) {
    val payload = if (body != null) Json.encodeToString(body) else "null"
    val message = Json.encodeToString(ResponseMessage(cid, payload))
    keydb.pub("api-gateway:response-channel", message)
}

private val logger = LoggerFactory.getLogger("UserService")

fun generateToken(userId: UUID, email: String): String {
    val algorithm = Algorithm.HMAC256("secret")
    return JWT.create()
        .withIssuer("issuer")
        .withClaim("userId", userId.toString())
        .withClaim("email", email)
        .withExpiresAt(Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000))
        .sign(algorithm)
}

fun initDatabase() {
    val config =
        HikariConfig().apply {
            jdbcUrl =
                System.getenv("DB_URL")
                    ?: "jdbc:postgresql://localhost:7777/postgres"
            driverClassName = "org.postgresql.Driver"
            username = System.getenv("DB_USER") ?: "postgres"
            password = System.getenv("DB_PASSWORD") ?: "postgres"
            maximumPoolSize = 10
        }
    val dataSource = HikariDataSource(config)

    Database.connect(dataSource)
    transaction {
        logger.info("Создаём (при необходимости) таблицы...")
        createMissingTablesAndColumns(Users)
        createMissingTablesAndColumns(UserMetricsHistory)
    }
    logger.info("База данных инициализирована успешно.")
}

fun main() {
    initDatabase()

    val keydb = KeyDBClient()

    GlobalScope.launch {
        logger.debug(
            "Ожидание сообщений в очереди user-service:request-queue..."
        )
        keydb.pop("user-service:request-queue") { requestJson ->
            logger.info("Получено сообщение из Redis: $requestJson")
            val requestMsg = Json.decodeFromString<RequestMessage>(requestJson)
            logger.info(
                "Тип запроса: ${requestMsg.type}, correlationId: ${requestMsg.correlationId}"
            )

            when (requestMsg.type) {
                "register" -> {
                    val registerRequest =
                        Json.decodeFromString<RegisterRequest>(
                            requestMsg.payload
                        )
                    logger.debug(
                        "Обрабатываем регистрацию пользователя: ${registerRequest.email}"
                    )
                    val authResponse: AuthResponse = transaction {
                        val existingUser =
                            Users.select {
                                    Users.email eq registerRequest.email
                                }
                                .singleOrNull()
                        if (existingUser != null) {
                            logger.warn(
                                "Пользователь с email ${registerRequest.email} уже существует."
                            )
                            AuthResponse(
                                "",
                                "Пользователь с таким email уже существует",
                                requestMsg.correlationId,
                            )
                        } else {
                            val hashedPassword =
                                BCrypt.hashpw(
                                    registerRequest.password,
                                    BCrypt.gensalt(),
                                )
                            val newUserId = UUID.randomUUID()
                            Users.insert {
                                it[id] = newUserId
                                it[email] = registerRequest.email
                                it[passwordHash] = hashedPassword
                                it[name] = registerRequest.name
                            }
                            val token =
                                generateToken(newUserId, registerRequest.email)
                            logger.info(
                                "Регистрация успешна: ${registerRequest.email}, userId: $newUserId"
                            )
                            AuthResponse(
                                token,
                                "Пользователь успешно зарегистрирован",
                                requestMsg.correlationId,
                            )
                        }
                    }
                    val responsePayload = Json.encodeToString(authResponse)
                    val responseMsg =
                        ResponseMessage(
                            requestMsg.correlationId,
                            responsePayload,
                        )
                    val responseJson = Json.encodeToString(responseMsg)
                    keydb.pub("api-gateway:response-channel", responseJson)
                    logger.debug(
                        "Опубликован ответ в канал api-gateway:response-channel"
                    )
                }
                "login" -> {
                    val loginRequest =
                        Json.decodeFromString<LoginRequest>(requestMsg.payload)
                    logger.debug(
                        "Обрабатываем вход пользователя: ${loginRequest.email}"
                    )
                    val authResponse: AuthResponse = transaction {
                        val userRow =
                            Users.select { Users.email eq loginRequest.email }
                                .singleOrNull()
                        if (
                            userRow != null &&
                                BCrypt.checkpw(
                                    loginRequest.password,
                                    userRow[Users.passwordHash],
                                )
                        ) {
                            val userId = userRow[Users.id]
                            logger.info(
                                "Успешный логин: ${loginRequest.email}, userId: $userId"
                            )
                            val token =
                                generateToken(userId, loginRequest.email)
                            AuthResponse(
                                token,
                                "Пользователь успешно аутентифицирован",
                                requestMsg.correlationId,
                            )
                        } else {
                            logger.warn(
                                "Неудачная попытка логина: ${loginRequest.email}"
                            )
                            AuthResponse(
                                "",
                                "Неверные учетные данные",
                                requestMsg.correlationId,
                            )
                        }
                    }
                    val responsePayload = Json.encodeToString(authResponse)
                    val responseMsg =
                        ResponseMessage(
                            requestMsg.correlationId,
                            responsePayload,
                        )
                    val responseJson = Json.encodeToString(responseMsg)
                    keydb.pub("api-gateway:response-channel", responseJson)
                    logger.debug(
                        "Опубликован ответ на логин в канал api-gateway:response-channel"
                    )
                }
                "get-profile-info" -> {
                    val userId = UUID.fromString(requestMsg.payload)
                    val info = transaction {
                        Users.select { Users.id eq userId }
                            .singleOrNull()
                            ?.let { row ->
                                UserInfo(
                                    id = row[Users.id].toString(),
                                    email = row[Users.email],
                                    name = row[Users.name],
                                    createdAt = row[Users.createdAt].toString(),
                                )
                            }
                    }
                    sendResponse(requestMsg.correlationId, info, keydb)
                }
                "get-profile-metrics" -> {
                    val userId = UUID.fromString(requestMsg.payload)
                    val metrics = transaction {
                        Users.select { Users.id eq userId }
                            .singleOrNull()
                            ?.let { row ->
                                UserMetrics(
                                    height = row[Users.height],
                                    weight = row[Users.weight],
                                    goal = row[Users.goal],
                                    activityLevel = row[Users.activityLevel],
                                )
                            }
                    }
                    sendResponse(requestMsg.correlationId, metrics, keydb)
                }
                "update-profile-info" -> {
                    val (userIdRaw, updateJson) =
                        requestMsg.payload.split(";", limit = 2)
                    val upd =
                        Json.decodeFromString<UpdateInfoRequest>(updateJson)
                    val profile = transaction {
                        Users.update({
                            Users.id eq UUID.fromString(userIdRaw)
                        }) { row ->
                            row[name] = upd.name
                        }
                        Users.select { Users.id eq UUID.fromString(userIdRaw) }
                            .singleOrNull()
                            ?.toProfile()
                    }
                    sendResponse(requestMsg.correlationId, profile, keydb)
                }
                "update-profile-metrics" -> {
                    val (userIdRaw, updateJson) =
                        requestMsg.payload.split(";", limit = 2)
                    val upd =
                        Json.decodeFromString<UpdateMetricsRequest>(updateJson)
                    val userId = UUID.fromString(userIdRaw)

                    val profile = transaction {
                        val existing =
                            Users.select { Users.id eq userId }.singleOrNull()

                        if (existing != null) {

                            val prevHeight = existing[Users.height]
                            if (
                                upd.height != null &&
                                    (prevHeight == null ||
                                        upd.height != prevHeight)
                            ) {
                                UserMetricsHistory.insert { stmt ->
                                    stmt[UserMetricsHistory.userId] = userId
                                    stmt[field] = "height"
                                    stmt[oldValue] = prevHeight ?: upd.height
                                    stmt[newValue] = upd.height
                                }
                            }

                            val prevWeight = existing[Users.weight]
                            if (
                                upd.weight != null &&
                                    (prevWeight == null ||
                                        upd.weight != prevWeight)
                            ) {
                                UserMetricsHistory.insert { stmt ->
                                    stmt[UserMetricsHistory.userId] = userId
                                    stmt[field] = "weight"
                                    stmt[oldValue] = prevWeight ?: upd.weight
                                    stmt[newValue] = upd.weight
                                }
                            }

                            Users.update({ Users.id eq userId }) { row ->
                                upd.height?.let { row[height] = it }
                                upd.weight?.let { row[weight] = it }
                                upd.goal?.let { row[goal] = it }
                                upd.activityLevel?.let {
                                    row[activityLevel] = it
                                }
                            }

                            Users.select { Users.id eq userId }
                                .singleOrNull()
                                ?.toProfile()
                        } else null
                    }

                    sendResponse<UserProfile>(
                        requestMsg.correlationId,
                        profile,
                        keydb,
                    )
                }
                "get-metrics-history" -> {
                    val (userIdRaw, queryJson) =
                        requestMsg.payload.split(";", limit = 2)
                    val userId = UUID.fromString(userIdRaw)

                    val params =
                        Json.decodeFromString<HistoryQueryParams>(queryJson)

                    val result = transaction {
                        val baseQuery =
                            UserMetricsHistory.select {
                                UserMetricsHistory.userId eq userId
                            }

                        val filtered =
                            baseQuery.let { q0 ->
                                var q = q0
                                params.field?.let {
                                    q =
                                        q.andWhere {
                                            UserMetricsHistory.field eq it
                                        }
                                }
                                params.from?.let {
                                    val fromDt =
                                        LocalDate.parse(it).atStartOfDay()
                                    q =
                                        q.andWhere {
                                            UserMetricsHistory
                                                .changedAt greaterEq fromDt
                                        }
                                }
                                params.to?.let {
                                    val toDt =
                                        LocalDate.parse(it).atTime(23, 59, 59)
                                    q =
                                        q.andWhere {
                                            UserMetricsHistory.changedAt lessEq
                                                toDt
                                        }
                                }
                                q
                            }

                        filtered
                            .orderBy(
                                UserMetricsHistory.changedAt to SortOrder.DESC
                            )
                            .map {
                                HistoryRecord(
                                    field = it[UserMetricsHistory.field],
                                    oldValue = it[UserMetricsHistory.oldValue],
                                    newValue = it[UserMetricsHistory.newValue],
                                    changedAt =
                                        it[UserMetricsHistory.changedAt]
                                            .toString(),
                                )
                            }
                    }

                    sendResponse(requestMsg.correlationId, result, keydb)
                }
            }
        }
    }

    embeddedServer(Netty, port = 8081) {
            install(ContentNegotiation) { json() }
            routing {
                get("/status") {
                    call.respondText(
                        "User Service is running with PostgreSQL",
                        ContentType.Text.Plain,
                    )
                }
            }
        }
        .start(wait = true)
}
