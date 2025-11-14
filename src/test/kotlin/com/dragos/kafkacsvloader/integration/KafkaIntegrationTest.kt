package com.dragos.kafkacsvloader.integration

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.csv.CsvParser
import com.dragos.kafkacsvloader.kafka.KafkaProducerClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.time.Duration
import java.util.Properties

class KafkaIntegrationTest {

    companion object {
        private lateinit var network: Network
        private lateinit var kafka: KafkaContainer
        private lateinit var schemaRegistry: GenericContainer<*>
        
        private lateinit var bootstrapServers: String
        private lateinit var schemaRegistryUrl: String

        @JvmStatic
        @BeforeAll
        fun setup() {
            network = Network.newNetwork()

            // Start Kafka
            kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
            kafka.start()

            // Start Schema Registry
            schemaRegistry = GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.3"))
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            schemaRegistry.start()

            bootstrapServers = kafka.bootstrapServers
            schemaRegistryUrl = "http://"+schemaRegistry.host+":"+schemaRegistry.getMappedPort(8081)+""

            println("Kafka started at: $bootstrapServers")
            println("Schema Registry started at: $schemaRegistryUrl")
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            schemaRegistry.stop()
            kafka.stop()
            network.close()
        }
    }

    @TempDir
    lateinit var tempDir: File

    @Test
    fun `should load CSV data into Kafka with Avro schema end-to-end`() {
        // Given: Create test schema
        val schemaContent = """
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
        val schemaFile = File(tempDir, "user.avsc").apply {
            writeText(schemaContent)
        }
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

        // Given: Create test CSV
        val csvContent = """
            id,name,email,age,active
            1,Alice,alice@example.com,30,true
            2,Bob,bob@example.com,25,false
            3,Charlie,charlie@example.com,35,true
        """.trimIndent()
        val csvFile = File(tempDir, "users.csv").apply {
            writeText(csvContent)
        }
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // Given: Kafka topic
        val topic = "test-users-"+System.currentTimeMillis()

        // When: Send data to Kafka
        KafkaProducerClient(bootstrapServers, schemaRegistryUrl).use { producer ->
            csvData.rows.forEach { row ->
                val result = AvroRecordMapper.mapRow(schema, row)
                if (result is com.dragos.kafkacsvloader.avro.RowMappingResult.Success) {
                    val key = row["id"]
                    producer.sendSync(topic, key, result.record)
                }
            }
        }

        // Then: Consume and verify
        val consumer = createConsumer()
        consumer.subscribe(listOf(topic))

        val records = mutableListOf<GenericRecord>()
        val startTime = System.currentTimeMillis()
        val timeout = 30_000L // 30 seconds

        while (records.size < 3 && (System.currentTimeMillis() - startTime) < timeout) {
            val polled = consumer.poll(Duration.ofSeconds(2))
            polled.forEach { record ->
                records.add(record.value() as GenericRecord)
            }
        }

        consumer.close()

        // Verify we received all 3 records
        records.size shouldBe 3

        // Verify first record
        val alice = records.find { (it.get("name") as String) == "Alice" }
        alice shouldBe org.junit.jupiter.api.Assertions.assertNotNull(alice)
        alice?.get("id") shouldBe 1
        alice?.get("email") shouldBe "alice@example.com"
        alice?.get("age") shouldBe 30
        alice?.get("active") shouldBe true

        // Verify second record
        val bob = records.find { (it.get("name") as String) == "Bob" }
        bob shouldBe org.junit.jupiter.api.Assertions.assertNotNull(bob)
        bob?.get("id") shouldBe 2
        bob?.get("email") shouldBe "bob@example.com"
        bob?.get("age") shouldBe 25
        bob?.get("active") shouldBe false

        // Verify third record
        val charlie = records.find { (it.get("name") as String) == "Charlie" }
        charlie shouldBe org.junit.jupiter.api.Assertions.assertNotNull(charlie)
        charlie?.get("id") shouldBe 3
        charlie?.get("email") shouldBe "charlie@example.com"
        charlie?.get("age") shouldBe 35
        charlie?.get("active") shouldBe true
    }

    @Test
    fun `should handle validation errors gracefully`() {
        // Given: Create test schema
        val schemaContent = """
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
        val schemaFile = File(tempDir, "user.avsc").apply {
            writeText(schemaContent)
        }
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

        // Given: CSV with invalid data
        val csvContent = """
            id,name
            not-a-number,Alice
        """.trimIndent()
        val csvFile = File(tempDir, "invalid.csv").apply {
            writeText(csvContent)
        }
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // When: Try to map invalid row
        val result = AvroRecordMapper.mapRow(schema, csvData.rows.first())

        // Then: Should fail with validation error
        result shouldBe org.junit.jupiter.api.Assertions.assertInstanceOf(
            com.dragos.kafkacsvloader.avro.RowMappingResult.Failure::class.java, 
            result
        )
    }

    private fun createConsumer(): KafkaConsumer<String, GenericRecord> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-"+System.currentTimeMillis())
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java.name)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false)
        }
        return KafkaConsumer(props)
    }
}