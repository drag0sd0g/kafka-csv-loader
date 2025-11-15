package com.dragos.kafkacsvloader.integration

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.avro.RowMappingResult
import com.dragos.kafkacsvloader.csv.CsvParser
import com.dragos.kafkacsvloader.kafka.KafkaProducerClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.time.Duration
import java.util.Properties

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaIntegrationTest {
    private lateinit var network: Network
    private lateinit var kafka: KafkaContainer
    private lateinit var schemaRegistry: GenericContainer<*>

    private lateinit var bootstrapServers: String
    private lateinit var schemaRegistryUrl: String

    @BeforeAll
    fun setup() {
        println("Starting Testcontainers setup...")

        network = Network.newNetwork()
        println("✓ Network created")

        // Start Kafka
        kafka =
            KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
        println("Starting Kafka container...")
        kafka.start()
        println("✓ Kafka started")

        // Start Schema Registry
        schemaRegistry =
            GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.3"))
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
        println("Starting Schema Registry container...")
        schemaRegistry.start()
        println("✓ Schema Registry started")

        bootstrapServers = kafka.bootstrapServers
        schemaRegistryUrl = "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(8081)}"

        println("✓✓ All containers ready!")
        println("   Kafka: $bootstrapServers")
        println("   Schema Registry: $schemaRegistryUrl")
    }

    @AfterAll
    fun teardown() {
        println("Stopping containers...")
        schemaRegistry.stop()
        kafka.stop()
        network.close()
        println("✓ Cleanup complete")
    }

    @TempDir
    lateinit var tempDir: File

    @Test
    fun `should load CSV data into Kafka with Avro schema end-to-end`() {
        println("\n=== Running end-to-end test ===")

        // Given: Create test schema
        val schemaContent =
            """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.dragos.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """.trimIndent()
        val schemaFile =
            File(tempDir, "user.avsc").apply {
                writeText(schemaContent)
            }
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)
        println("✓ Schema loaded")

        // Given: Create test CSV
        val csvContent =
            """
            id,name,email,age,active
            1,Alice,alice@example.com,30,true
            2,Bob,bob@example.com,25,false
            3,Charlie,charlie@example.com,35,true
            """.trimIndent()
        val csvFile =
            File(tempDir, "users.csv").apply {
                writeText(csvContent)
            }
        val csvData = CsvParser.parse(csvFile.absolutePath)
        println("✓ CSV parsed: ${csvData.rows.size} rows")

        // Given: Kafka topic
        val topic = "test-users-${System.currentTimeMillis()}"
        println("✓ Topic: $topic")

        // When: Send data to Kafka
        println("Sending records to Kafka...")
        KafkaProducerClient(bootstrapServers, schemaRegistryUrl).use { producer ->
            csvData.rows.forEach { row ->
                val result = AvroRecordMapper.mapRow(schema, row)
                if (result is RowMappingResult.Success) {
                    val key = row["id"]
                    producer.sendSync(topic, key, result.record)
                    println("  ✓ Sent record with key: $key")
                }
            }
        }

        // Then: Consume and verify
        println("Consuming records from Kafka...")
        val consumer = createConsumer()
        consumer.subscribe(listOf(topic))

        val records = mutableListOf<GenericRecord>()
        val startTime = System.currentTimeMillis()
        val timeout = 30_000L // 30 seconds

        while (records.size < 3 && (System.currentTimeMillis() - startTime) < timeout) {
            val polled = consumer.poll(Duration.ofSeconds(2))
            polled.forEach { record ->
                records.add(record.value() as GenericRecord)
                println("  ✓ Consumed record: ${record.value()}")
            }
        }

        consumer.close()

        // Verify we received all 3 records
        println("Verifying ${records.size} records...")
        records.size shouldBe 3

        // Helper function to convert Avro Utf8 to String
        fun GenericRecord.getString(field: String): String = this.get(field).toString()

        // Verify first record
        val alice = records.find { it.getString("name") == "Alice" }
        alice shouldNotBe null
        alice?.get("id") shouldBe 1
        alice?.getString("email") shouldBe "alice@example.com"
        alice?.get("age") shouldBe 30
        alice?.get("active") shouldBe true
        println("  ✓ Alice verified")

        // Verify second record
        val bob = records.find { it.getString("name") == "Bob" }
        bob shouldNotBe null
        bob?.get("id") shouldBe 2
        bob?.getString("email") shouldBe "bob@example.com"
        bob?.get("age") shouldBe 25
        bob?.get("active") shouldBe false
        println("  ✓ Bob verified")

        // Verify third record
        val charlie = records.find { it.getString("name") == "Charlie" }
        charlie shouldNotBe null
        charlie?.get("id") shouldBe 3
        charlie?.getString("email") shouldBe "charlie@example.com"
        charlie?.get("age") shouldBe 35
        charlie?.get("active") shouldBe true
        println("  ✓ Charlie verified")

        println("=== End-to-end test PASSED ===\n")
    }

    @Test
    fun `should handle validation errors gracefully`() {
        println("\n=== Running validation error test ===")

        // Given: Create test schema
        val schemaContent =
            """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.dragos.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
            """.trimIndent()
        val schemaFile =
            File(tempDir, "user.avsc").apply {
                writeText(schemaContent)
            }
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

        // Given: CSV with invalid data
        val csvContent =
            """
            id,name
            not-a-number,Alice
            """.trimIndent()
        val csvFile =
            File(tempDir, "invalid.csv").apply {
                writeText(csvContent)
            }
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // When: Try to map invalid row
        val result = AvroRecordMapper.mapRow(schema, csvData.rows.first())

        // Then: Should fail with validation error
        Assertions.assertTrue(result is RowMappingResult.Failure)
        println("✓ Validation error handled correctly")
        println("=== Validation error test PASSED ===\n")
    }

    private fun createConsumer(): KafkaConsumer<String, GenericRecord> {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-${System.currentTimeMillis()}")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java.name)
                put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false)
            }
        return KafkaConsumer(props)
    }
}
