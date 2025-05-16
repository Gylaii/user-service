plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("io.ktor.plugin") version "2.3.0"
    id("com.ncorti.ktfmt.gradle") version "0.22.0"
    application
    idea
    java
}

application { mainClass.set("AppKt") }

repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("io.ktor:ktor-server-core:2.3.0")
    implementation("io.ktor:ktor-server-netty:2.3.0")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.0")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.0")
    implementation("io.ktor:ktor-server-call-logging:2.3.0")
    implementation("io.ktor:ktor-server-status-pages:2.3.0")

    implementation("io.lettuce:lettuce-core:6.2.4.RELEASE")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.5.1")

    implementation("com.auth0:java-jwt:4.4.0")

    implementation("org.jetbrains.exposed:exposed-core:0.41.1")
    implementation("org.jetbrains.exposed:exposed-dao:0.41.1")
    implementation("org.jetbrains.exposed:exposed-jdbc:0.41.1")
    implementation("org.jetbrains.exposed:exposed-java-time:0.41.1")

    implementation("org.postgresql:postgresql:42.7.2")

    implementation("com.zaxxer:HikariCP:5.0.1")

    implementation("org.mindrot:jbcrypt:0.4")

    implementation("ch.qos.logback:logback-classic:1.4.14")

    implementation("com.github.Gylaii:keydb-client-lib:v0.1.1")

    testImplementation("io.ktor:ktor-server-tests:2.3.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.8.22")
}

ktfmt {
    kotlinLangStyle()
    maxWidth.set(80)
    removeUnusedImports.set(false)
    manageTrailingCommas.set(true)
}
