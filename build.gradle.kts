import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.22"
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.0"
    jacoco
    id("pl.allegro.tech.build.axion-release") version "1.17.0"
}

group = "com.dragos"
version = scmVersion.version

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

    // Set Docker socket for Testcontainers when using Colima
    val dockerSocket = "${System.getProperty("user.home")}/.colima/default/docker.sock"
    environment("DOCKER_HOST", "unix://$dockerSocket")
    environment("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", dockerSocket)

    // IMPORTANT: Disable Ryuk for Colima compatibility
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")

    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = true
    }

    // Generate coverage report after tests
    finalizedBy(tasks.jacocoTestReport)
}

// Fat JAR configuration
tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.dragos.kafkacsvloader.MainKt"
        attributes["Implementation-Version"] = version
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

// ktlint configuration
ktlint {
    version.set("1.0.1")
    verbose.set(true)
    android.set(false)
    outputToConsole.set(true)
    ignoreFailures.set(false)

    filter {
        exclude("**/generated/**")
        exclude("**/build/**")
        include("**/kotlin/**")
    }
}

// Fix ktlint dependency on Avro generation
tasks.named("runKtlintCheckOverMainSourceSet") {
    dependsOn("generateAvroJava")
}

tasks.named("runKtlintCheckOverTestSourceSet") {
    dependsOn("generateTestAvroJava")
}

tasks.named("runKtlintFormatOverMainSourceSet") {
    dependsOn("generateAvroJava")
}

tasks.named("runKtlintFormatOverTestSourceSet") {
    dependsOn("generateTestAvroJava")
}

// JaCoCo configuration
jacoco {
    toolVersion = "0.8.11"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)

    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }

    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it) {
                    exclude(
                        "**/generated/**",
                    )
                }
            },
        ),
    )
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)

    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }

    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it) {
                    exclude(
                        "**/generated/**",
                        "**/Main.class",
                        "**/MainKt.class",
                        "**/MainKt\$*.class",
                        "**/KafkaCsvLoaderCommand.class",
                        "**/KafkaCsvLoaderCommand\$*.class",
                        "**/LoadCommand.class",
                        "**/LoadCommand\$*.class",
                    )
                }
            },
        ),
    )
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.jacocoTestReport)

    // Use the same exclusions as the report
    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it) {
                    exclude(
                        "**/generated/**",
                        "**/Main.class",
                        "**/MainKt.class",
                        "**/MainKt\$*.class",
                        "**/KafkaCsvLoaderCommand.class",
                        "**/KafkaCsvLoaderCommand\$*.class",
                        "**/LoadCommand.class",
                        "**/LoadCommand\$*.class",
                    )
                }
            },
        ),
    )

    violationRules {
        rule {
            element = "BUNDLE"
            limit {
                counter = "INSTRUCTION"
                value = "COVEREDRATIO"
                // Should be high since CLI is excluded
                minimum = "0.80".toBigDecimal()
            }
        }

        rule {
            element = "CLASS"
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.70".toBigDecimal()
            }
        }
    }
}

// Make build depend on ktlint checks
tasks.named("check") {
    dependsOn("ktlintCheck")
    dependsOn("jacocoTestCoverageVerification")
}

// Auto-format code before compiling
tasks.named("compileKotlin") {
    dependsOn("ktlintFormat")
}

tasks.named("compileTestKotlin") {
    dependsOn("ktlintFormat")
}
