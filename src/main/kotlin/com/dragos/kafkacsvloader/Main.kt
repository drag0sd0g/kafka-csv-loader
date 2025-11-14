package com.dragos.kafkacsvloader

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.avro.RowMappingResult
import com.dragos.kafkacsvloader.csv.CsvParser
import com.dragos.kafkacsvloader.kafka.KafkaProducerClient
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.mordant.rendering.TextColors.*
import com.github.ajalt.mordant.rendering.TextStyles.*
import com.github.ajalt.mordant.terminal.Terminal
import kotlin.system.exitProcess

class KafkaCsvLoaderCommand : CliktCommand(
    name = "kafka-csv-loader",
    help = "Load CSV data into Kafka with Avro schema validation"
) {
    private val csvFile by option("--csv", "-c", help = "Path to CSV file").required()
    private val schemaFile by option("--schema", "-s", help = "Path to Avro schema file (.avsc)").required()
    private val topic by option("--topic", "-t", help = "Kafka topic name").required()
    private val bootstrapServers by option(
        "--bootstrap-servers", "-b",
        help = "Kafka bootstrap servers"
    ).default("localhost:9092")
    private val schemaRegistry by option(
        "--schema-registry", "-r",
        help = "Schema Registry URL"
    ).default("http://localhost:8081")
    private val keyField by option(
        "--key-field", "-k",
        help = "CSV column to use as Kafka message key (optional)"
    )

    private val terminal = Terminal()

    override fun run() {
        terminal.println(bold(cyan("🚀 Kafka CSV Loader")))
        terminal.println()

        try {
            // Step 1: Load schema
            terminal.print(yellow("📋 Loading Avro schema... "))
            val schema = AvroSchemaLoader.loadFromFile(schemaFile)
            terminal.println(green("✓"))
            terminal.println("   Schema: ${schema.namespace}.${schema.name}")
            terminal.println("   Fields: ${schema.fields.joinToString(", ") { it.name() }}")
            terminal.println()

            // Step 2: Parse CSV
            terminal.print(yellow("📄 Parsing CSV file... "))
            val csvData = CsvParser.parse(csvFile)
            terminal.println(green("✓"))
            terminal.println("   Headers: ${csvData.headers.joinToString(", ")}")
            terminal.println("   Rows: ${csvData.rows.size}")
            terminal.println()

            // Step 3: Validate headers
            terminal.print(yellow("🔍 Validating CSV headers against schema... "))
            val schemaFields = schema.fields.map { it.name() }
            if (!CsvParser.validateHeaders(csvData.headers, schemaFields)) {
                val missing = CsvParser.getMissingFields(csvData.headers, schemaFields)
                terminal.println(red("✗"))
                terminal.println(red("   Missing required fields: ${missing.joinToString(", ")}"))
                exitProcess(1)
            }
            terminal.println(green("✓"))
            terminal.println()

            // Step 4: Connect to Kafka
            terminal.print(yellow("🔌 Connecting to Kafka... "))
            terminal.println()
            terminal.println("   Bootstrap servers: $bootstrapServers")
            terminal.println("   Schema Registry: $schemaRegistry")
            terminal.println("   Topic: $topic")
            terminal.println()

            // Step 5: Process and send records
            KafkaProducerClient(bootstrapServers, schemaRegistry).use { producer ->
                terminal.println(yellow("📤 Sending records to Kafka..."))
                terminal.println()

                var successCount = 0
                var failureCount = 0
                val failures = mutableListOf<Pair<Int, String>>()

                csvData.rows.forEachIndexed { index, row ->
                    val rowNumber = index + 1
                    val result = AvroRecordMapper.mapRow(schema, row)

                    when (result) {
                        is RowMappingResult.Success -> {
                            val key = keyField?.let { row[it] }
                            try {
                                producer.sendSync(topic, key, result.record)
                                successCount++
                                terminal.print(green("✓"))
                                if (rowNumber % 50 == 0) {
                                    terminal.println(" $rowNumber")
                                }
                            } catch (e: Exception) {
                                failureCount++
                                failures.add(rowNumber to "Kafka error: ${e.message}")
                                terminal.print(red("✗"))
                            }
                        }
                        is RowMappingResult.Failure -> {
                            failureCount++
                            failures.add(rowNumber to result.errors.joinToString("; "))
                            terminal.print(red("✗"))
                        }
                    }
                }
                terminal.println()
                terminal.println()

                // Step 6: Summary
                terminal.println(bold(cyan("📊 Summary")))
                terminal.println(green("   ✓ Success: $successCount"))
                if (failureCount > 0) {
                    terminal.println(red("   ✗ Failures: $failureCount"))
                    terminal.println()
                    terminal.println(yellow("   Failed rows:"))
                    failures.take(10).forEach { (rowNum, error) ->
                        terminal.println(red("     Row $rowNum: $error"))
                    }
                    if (failures.size > 10) {
                        terminal.println(yellow("     ... and ${failures.size - 10} more"))
                    }
                    exitProcess(1)
                }
                terminal.println()
                terminal.println(bold(green("✅ All records successfully loaded!")))
            }

        } catch (e: Exception) {
            terminal.println()
            terminal.println(red(bold("❌ Error: ${e.message}")))
            if (e.cause != null) {
                terminal.println(red("   Caused by: ${e.cause?.message}"))
            }
            exitProcess(1)
        }
    }
}

fun main(args: Array<String>) = KafkaCsvLoaderCommand().main(args)