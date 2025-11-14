package com.dragos.kafkacsvloader.cli

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.avro.RowMappingResult
import com.dragos.kafkacsvloader.csv.CsvParser
import com.dragos.kafkacsvloader.kafka.KafkaProducerClient
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.mordant.rendering.TextColors.*
import com.github.ajalt.mordant.terminal.Terminal
import org.slf4j.LoggerFactory
import java.util.UUID

class LoadCommand : CliktCommand(
    name = "load",
    help = "Load CSV data into Kafka with Avro schema validation"
) {
    private val csvPath by option("--csv", "-c", help = "Path to CSV file").required()
    private val topic by option("--topic", "-t", help = "Kafka topic name").required()
    private val schemaPath by option("--schema", "-s", help = "Path to Avro schema file (.avsc)").required()
    private val bootstrapServers by option("--bootstrap-servers", "-b", help = "Kafka bootstrap servers")
        .default("localhost:9092")
    private val schemaRegistryUrl by option("--schema-registry", "-r", help = "Schema Registry URL")
        .default("http://localhost:8081")
    private val keyColumn by option("--key-column", "-k", help = "CSV column to use as Kafka record key (optional)")
    private val dryRun by option("--dry-run", help = "Validate and map rows without sending to Kafka").flag()

    private val terminal = Terminal()
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run() {
        terminal.println(brightBlue("=== Kafka CSV Loader ==="))
        terminal.println()
        
        printConfiguration()
        
        try {
            // Step 1: Load Avro schema
            terminal.println(yellow("Loading Avro schema..."))
            val schema = AvroSchemaLoader.loadFromFile(schemaPath)
            terminal.println(green("✓ Schema loaded: ${schema.name}"))
            terminal.println()

            // Step 2: Parse CSV
            terminal.println(yellow("Parsing CSV file..."))
            val csv = CsvParser.parse(csvPath)
            terminal.println(green("✓ CSV parsed: ${csv.rows.size} rows, ${csv.headers.size} columns"))
            terminal.println()

            // Step 3: Validate headers
            terminal.println(yellow("Validating CSV headers against schema..."))
            val schemaFields = schema.fields.map { it.name() }
            if (!CsvParser.validateHeaders(csv.headers, schemaFields)) {
                val missing = CsvParser.getMissingFields(csv.headers, schemaFields)
                terminal.println(red("✗ Validation failed: CSV is missing required schema fields: $missing"))
                throw IllegalArgumentException("CSV headers do not match schema fields")
            }
            terminal.println(green("✓ Headers validated"))
            terminal.println()

            // Step 4: Process rows
            if (dryRun) {
                terminal.println(cyan("DRY RUN MODE: Validating and mapping rows (not sending to Kafka)"))
            } else {
                terminal.println(yellow("Processing rows and sending to Kafka..."))
            }
            terminal.println()

            val producer = if (!dryRun) {
                KafkaProducerClient(bootstrapServers, schemaRegistryUrl)
            } else null

            var successCount = 0
            val failures = mutableListOf<Pair<Int, List<String>>>()

            try {
                csv.rows.forEachIndexed { index, row ->
                    val rowNumber = index + 1
                    val key = keyColumn?.let { row[it] } ?: UUID.randomUUID().toString()

                    when (val result = AvroRecordMapper.mapRow(schema, row)) {
                        is RowMappingResult.Success -> {
                            if (dryRun) {
                                terminal.println("  Row $rowNumber: ${green("✓")} Mapped successfully")
                                log.debug("Row {} mapped: {}", rowNumber, result.record)
                            } else {
                                try {
                                    producer!!.sendSync(topic, key, result.record)
                                    successCount++
                                    if (rowNumber % 100 == 0) {
                                        terminal.println("  Processed $rowNumber rows...")
                                    }
                                } catch (e: Exception) {
                                    failures.add(rowNumber to listOf("Kafka send failed: ${e.message}"))
                                    log.error("Failed to send row {}", rowNumber, e)
                                }
                            }
                        }
                        is RowMappingResult.Failure -> {
                            failures.add(rowNumber to result.errors)
                            terminal.println("  Row $rowNumber: ${red("✗")} ${result.errors.joinToString(", ")}")
                        }
                    }
                }
            } finally {
                producer?.close()
            }

            // Step 5: Print summary
            terminal.println()
            terminal.println(brightBlue("=== Summary ==="))
            terminal.println("Total rows: ${csv.rows.size}")
            if (!dryRun) {
                terminal.println(green("Successfully sent: $successCount"))
            } else {
                terminal.println(green("Successfully validated: ${csv.rows.size - failures.size}"))
            }
            terminal.println(red("Failed: ${failures.size}"))

            if (failures.isNotEmpty()) {
                terminal.println()
                terminal.println(red("Failed rows:"))
                failures.take(10).forEach { (rowNum, errors) ->
                    terminal.println("  Row $rowNum: ${errors.joinToString(", ")}")
                }
                if (failures.size > 10) {
                    terminal.println("  ... and ${failures.size - 10} more")
                }
            }

            if (failures.isNotEmpty()) {
                throw IllegalStateException("${failures.size} rows failed validation or send")
            }

        } catch (e: Exception) {
            terminal.println()
            terminal.println(red("✗ Error: ${e.message}"))
            log.error("Load command failed", e)
            throw e
        }
    }

    private fun printConfiguration() {
        terminal.println(cyan("Configuration:"))
        terminal.println("  CSV file: $csvPath")
        terminal.println("  Kafka topic: $topic")
        terminal.println("  Avro schema: $schemaPath")
        terminal.println("  Bootstrap servers: $bootstrapServers")
        terminal.println("  Schema Registry: $schemaRegistryUrl")
        terminal.println("  Key column: ${keyColumn ?: "(auto-generated UUID)"}")
        terminal.println("  Dry run: $dryRun")
        terminal.println()
    }
}