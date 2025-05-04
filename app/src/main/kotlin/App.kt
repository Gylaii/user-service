import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString

import io.lettuce.core.RedisClient
import io.lettuce.core.api.sync.RedisCommands

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.SchemaUtils.createMissingTablesAndColumns
import org.jetbrains.exposed.sql.javatime.datetime

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import java.util.*
import java.time.LocalDateTime

import org.mindrot.jbcrypt.BCrypt

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.http.ContentType
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory

@Serializable
data class RegisterRequest(val email: String, val password: String, val name: String)

@Serializable
data class LoginRequest(val email: String, val password: String)

@Serializable
data class AuthResponse(val token: String, val message: String, val correlationId: String = "")

@Serializable
data class RequestMessage(val type: String, val correlationId: String, val payload: String)

@Serializable
data class ResponseMessage(val correlationId: String, val payload: String)

object Users : Table("users") {
    val id = uuid("id").clientDefault { UUID.randomUUID() }
    val email = varchar("email", 255).uniqueIndex()
    val passwordHash = varchar("password_hash", 255)
    val name = varchar("name", 255)
    val createdAt = datetime("created_at").clientDefault { LocalDateTime.now() }
    override val primaryKey = PrimaryKey(id)
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
    val config = HikariConfig().apply {
        jdbcUrl = System.getenv("DB_URL") ?: "jdbc:postgresql://localhost:7777/postgres"
        driverClassName = "org.postgresql.Driver"
        username = System.getenv("DB_USER") ?: "postgres"
        password = System.getenv("DB_PASSWORD") ?: "postgres"
        maximumPoolSize = 10
    }
    val dataSource = HikariDataSource(config)

    Database.connect(dataSource)
    transaction {
        logger.info("Создаём (при необходимости) таблицу `users` через createMissingTablesAndColumns...")
        createMissingTablesAndColumns(Users)
    }
    logger.info("База данных инициализирована успешно.")
}

fun main() {
    initDatabase()

    val redisClient = RedisClient.create("redis://${System.getenv("KEYDB_HOST") ?: "localhost"}:${System.getenv("KEYDB_PORT") ?: "6379"}")
    val connection = redisClient.connect()
    val commands: RedisCommands<String, String> = connection.sync()

    GlobalScope.launch {
        while (true) {
            logger.debug("Ожидание сообщений в очереди user-service:request-queue...")
            val result = commands.brpop(0, "user-service:request-queue")
            val requestJson = result?.value

            if (requestJson != null) {
                logger.info("Получено сообщение из Redis: $requestJson")
                val requestMsg = Json.decodeFromString<RequestMessage>(requestJson)
                logger.info("Тип запроса: ${requestMsg.type}, correlationId: ${requestMsg.correlationId}")

                when (requestMsg.type) {
                    "register" -> {
                        val registerRequest = Json.decodeFromString<RegisterRequest>(requestMsg.payload)
                        logger.debug("Обрабатываем регистрацию пользователя: ${registerRequest.email}")
                        val authResponse: AuthResponse = transaction {
                            val existingUser = Users.select { Users.email eq registerRequest.email }
                                .singleOrNull()
                            if (existingUser != null) {
                                logger.warn("Пользователь с email ${registerRequest.email} уже существует.")
                                AuthResponse("", "Пользователь с таким email уже существует", requestMsg.correlationId)
                            } else {
                                val hashedPassword = BCrypt.hashpw(registerRequest.password, BCrypt.gensalt())
                                val newUserId = UUID.randomUUID()
                                Users.insert {
                                    it[id] = newUserId
                                    it[email] = registerRequest.email
                                    it[passwordHash] = hashedPassword
                                    it[name] = registerRequest.name
                                }
                                val token = generateToken(newUserId, registerRequest.email)
                                logger.info("Регистрация успешна: ${registerRequest.email}, userId: $newUserId")
                                AuthResponse(token, "Пользователь успешно зарегистрирован", requestMsg.correlationId)
                            }
                        }
                        val responsePayload = Json.encodeToString(authResponse)
                        val responseMsg = ResponseMessage(requestMsg.correlationId, responsePayload)
                        val responseJson = Json.encodeToString(responseMsg)
                        commands.publish("api-gateway:response-channel", responseJson)
                        logger.debug("Опубликован ответ в канал api-gateway:response-channel")

                    }
                    "login" -> {
                        val loginRequest = Json.decodeFromString<LoginRequest>(requestMsg.payload)
                        logger.debug("Обрабатываем вход пользователя: ${loginRequest.email}")
                        val authResponse: AuthResponse = transaction {
                            val userRow = Users.select { Users.email eq loginRequest.email }
                                .singleOrNull()
                            if (userRow != null && BCrypt.checkpw(loginRequest.password, userRow[Users.passwordHash])) {
                                val userId = userRow[Users.id]
                                logger.info("Успешный логин: ${loginRequest.email}, userId: $userId")
                                val token = generateToken(userId, loginRequest.email)
                                AuthResponse(token, "Пользователь успешно аутентифицирован", requestMsg.correlationId)
                            } else {
                                logger.warn("Неудачная попытка логина: ${loginRequest.email}")
                                AuthResponse("", "Неверные учетные данные", requestMsg.correlationId)
                            }
                        }
                        val responsePayload = Json.encodeToString(authResponse)
                        val responseMsg = ResponseMessage(requestMsg.correlationId, responsePayload)
                        val responseJson = Json.encodeToString(responseMsg)
                        commands.publish("api-gateway:response-channel", responseJson)
                        logger.debug("Опубликован ответ на логин в канал api-gateway:response-channel")
                    }
                    else -> {
                        logger.warn("Неизвестный тип запроса: ${requestMsg.type}")
                    }
                }
            } else {
                logger.debug("В очереди user-service:request-queue нет сообщений. Продолжаем ожидание.")
            }
        }
    }

    embeddedServer(Netty, port = 8081) {
        install(ContentNegotiation) {
            json()
        }
        routing {
            get("/status") {
                call.respondText("User Service is running with PostgreSQL", ContentType.Text.Plain)
            }
        }
    }.start(wait = true)
}
