import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.22"
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.dragos"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    // Kotlin
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // CLI
    implementation("com.github.ajalt.clikt:clikt:4.2.1")
    implementation("com.github.ajalt.mordant:mordant:2.2.0")

    // CSV parsing
    implementation("com.github.doyaaaaaken:kotlin-csv-jvm:1.9.2")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.6.1")

    // Avro
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.confluent:kafka-avro-serializer:7.5.3")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("io.mockk:mockk:1.13.8")
}

application {
    mainClass.set("com.dragos.kafkacsvloader.MainKt")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "21"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Fat JAR configuration
tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.dragos.kafkacsvloader.MainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}

// Avro code generation
avro {
    setOutputCharacterEncoding("UTF-8")
    setStringType("String")
    fieldVisibility.set("PRIVATE")
}