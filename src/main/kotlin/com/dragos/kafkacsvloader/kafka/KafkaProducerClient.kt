package com.dragos.kafkacsvloader.kafka

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Future

/**
 * Kafka producer client configured to send Avro GenericRecords.
 * Uses Confluent's KafkaAvroSerializer which handles Schema Registry integration.
 */
class KafkaProducerClient(
    bootstrapServers: String,
    schemaRegistryUrl: String
) : AutoCloseable {
    
    private val log = LoggerFactory.getLogger(javaClass)
    private val producer: KafkaProducer<String, GenericRecord>

    init {
        val props = Properties().apply {
            // Kafka connection
            put("bootstrap.servers", bootstrapServers)
            
            // Serializers
            put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
            
            // Schema Registry
            put("schema.registry.url", schemaRegistryUrl)
            
            // Producer reliability settings
            put("acks", "all")
            put("retries", 3)
            put("max.in.flight.requests.per.connection", 5)
            put("enable.idempotence", true)
            
            // Performance tuning
            put("compression.type", "snappy")
            put("batch.size", 16384)
            put("linger.ms", 10)
        }
        
        producer = KafkaProducer(props)
        log.info("Kafka producer initialized with bootstrap.servers={}, schema.registry.url={}", 
                 bootstrapServers, schemaRegistryUrl)
    }

    /**
     * Send a GenericRecord to the specified topic.
     * Key can be null (will distribute round-robin).
     * Returns a Future that completes when the send finishes.
     */
    fun send(topic: String, key: String?, value: GenericRecord): Future<RecordMetadata> {
        val record = ProducerRecord(topic, key, value)
        log.debug("Sending record to topic={} key={}", topic, key)
        
        return producer.send(record) { metadata, exception ->
            if (exception != null) {
                log.error("Failed to send record to topic={} key={}", topic, key, exception)
            } else {
                log.debug("Record sent successfully: topic={}, partition={}, offset={}", 
                         metadata.topic(), metadata.partition(), metadata.offset())
            }
        }
    }

    /**
     * Synchronously send and wait for completion.
     * Throws exception if send fails.
     */
    fun sendSync(topic: String, key: String?, value: GenericRecord): RecordMetadata {
        return send(topic, key, value).get()
    }

    override fun close() {
        try {
            log.info("Closing Kafka producer (flushing pending records)")
            producer.flush()
            producer.close()
        } catch (e: Exception) {
            log.warn("Error closing Kafka producer", e)
        }
    }
}