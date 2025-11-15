package com.dragos.kafkacsvloader

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.avro.RowMappingResult
import com.dragos.kafkacsvloader.csv.CsvData
import com.dragos.kafkacsvloader.csv.CsvParser
import com.dragos.kafkacsvloader.kafka.KafkaProducerClient
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.versionOption
import com.github.ajalt.mordant.rendering.TextColors.cyan
import com.github.ajalt.mordant.rendering.TextColors.green
import com.github.ajalt.mordant.rendering.TextColors.red
import com.github.ajalt.mordant.rendering.TextColors.yellow
import com.github.ajalt.mordant.rendering.TextStyles.bold
import com.github.ajalt.mordant.terminal.Terminal
import org.apache.avro.Schema
import kotlin.system.exitProcess

class KafkaCsvLoaderCommand : CliktCommand(
    name = "kafka-csv-loader",
    help = "Load CSV data into Kafka with Avro schema validation",
) {
    private val csvFile by option("--csv", "-c", help = "Path to CSV file").required()
    private val schemaFile by option("--schema", "-s", help = "Path to Avro schema file (.avsc)").required()
    private val topic by option("--topic", "-t", help = "Kafka topic name").required()
    private val bootstrapServers by option(
        "--bootstrap-servers",
        "-b",
        help = "Kafka bootstrap servers",
    ).default("localhost:9092")
    private val schemaRegistry by option(
        "--schema-registry",
        "-r",
        help = "Schema Registry URL",
    ).default("http://localhost:8081")
    private val keyField by option(
        "--key-field",
        "-k",
        help = "CSV column to use as Kafka message key (optional)",
    )
    private val dryRun by option(
        "--dry-run",
        "-d",
        help = "Validate CSV and schema without sending to Kafka",
    ).flag(default = false)

    init {
        versionOption(getVersion())
    }

    private val terminal = Terminal()

    override fun run() {
        printHeader()

        try {
            val schema = loadSchema()
            val csvData = parseCsv()
            validateHeaders(schema, csvData)

            if (dryRun) {
                performDryRun(schema, csvData)
            } else {
                performKafkaLoad(schema, csvData)
            }
        } catch (e: Exception) {
            printError(e)
            exitProcess(1)
        }
    }

    private fun printHeader() {
        terminal.println(bold(cyan("🚀 Kafka CSV Loader")))
        if (dryRun) {
            terminal.println(yellow("   DRY RUN MODE - No data will be sent to Kafka"))
        }
        terminal.println()
    }

    private fun loadSchema(): Schema {
        terminal.print(yellow("📋 Loading Avro schema... "))
        val schema = AvroSchemaLoader.loadFromFile(schemaFile)
        terminal.println(green("✓"))
        terminal.println("   Schema: ${schema.namespace}.${schema.name}")
        terminal.println("   Fields: ${schema.fields.joinToString(", ") { it.name() }}")
        terminal.println()
        return schema
    }

    private fun parseCsv(): CsvData {
        terminal.print(yellow("📄 Parsing CSV file... "))
        val csvData = CsvParser.parse(csvFile)
        terminal.println(green("✓"))
        terminal.println("   Headers: ${csvData.headers.joinToString(", ")}")
        terminal.println("   Rows: ${csvData.rows.size}")
        terminal.println()
        return csvData
    }

    private fun validateHeaders(
        schema: Schema,
        csvData: CsvData,
    ) {
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
    }

    private fun performDryRun(
        schema: Schema,
        csvData: CsvData,
    ) {
        terminal.println(yellow("🔍 Validating all rows (dry run)..."))
        terminal.println()

        var validCount = 0
        val failures = mutableListOf<Pair<Int, String>>()

        csvData.rows.forEachIndexed { index, row ->
            val rowNumber = index + 1
            val result = AvroRecordMapper.mapRow(schema, row)

            when (result) {
                is RowMappingResult.Success -> {
                    validCount++
                    if (rowNumber % 50 == 0) {
                        terminal.println(green("   ✓ Validated $rowNumber rows..."))
                    }
                }
                is RowMappingResult.Failure -> {
                    failures.add(rowNumber to result.errors.joinToString("; "))
                }
            }
        }

        terminal.println()
        terminal.println(bold(cyan("📊 Dry Run Summary")))
        terminal.println(green("   ✓ Valid rows: $validCount"))
        terminal.println(red("   ✗ Invalid rows: ${failures.size}"))

        if (failures.isNotEmpty()) {
            printFailures(failures)
            exitProcess(1)
        }

        terminal.println()
        terminal.println(bold(green("✅ All rows validated successfully! Ready to load to Kafka.")))
    }

    private fun performKafkaLoad(
        schema: Schema,
        csvData: CsvData,
    ) {
        printKafkaConnection()

        KafkaProducerClient(bootstrapServers, schemaRegistry).use { producer ->
            terminal.println(yellow("📤 Sending records to Kafka..."))
            terminal.println()

            var successCount = 0
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
                            failures.add(rowNumber to "Kafka error: ${e.message}")
                            terminal.print(red("✗"))
                        }
                    }
                    is RowMappingResult.Failure -> {
                        failures.add(rowNumber to result.errors.joinToString("; "))
                        terminal.print(red("✗"))
                    }
                }
            }

            terminal.println()
            terminal.println()

            printKafkaSummary(successCount, failures)
        }
    }

    private fun printKafkaConnection() {
        terminal.print(yellow("🔌 Connecting to Kafka... "))
        terminal.println()
        terminal.println("   Bootstrap servers: $bootstrapServers")
        terminal.println("   Schema Registry: $schemaRegistry")
        terminal.println("   Topic: $topic")
        terminal.println()
    }

    private fun printKafkaSummary(
        successCount: Int,
        failures: List<Pair<Int, String>>,
    ) {
        terminal.println(bold(cyan("📊 Summary")))
        terminal.println(green("   ✓ Success: $successCount"))

        if (failures.isNotEmpty()) {
            terminal.println(red("   ✗ Failures: ${failures.size}"))
            printFailures(failures)
            exitProcess(1)
        }

        terminal.println()
        terminal.println(bold(green("✅ All records successfully loaded!")))
    }

    private fun printFailures(failures: List<Pair<Int, String>>) {
        terminal.println()
        terminal.println(yellow("   Invalid rows:"))
        failures.take(10).forEach { (rowNum, error) ->
            terminal.println(red("     Row $rowNum: $error"))
        }
        if (failures.size > 10) {
            terminal.println(yellow("     ... and ${failures.size - 10} more"))
        }
    }

    private fun printError(e: Exception) {
        terminal.println()
        terminal.println(red(bold("❌ Error: ${e.message}")))
        if (e.cause != null) {
            terminal.println(red("   Caused by: ${e.cause?.message}"))
        }
    }

    private fun getVersion(): String {
        return this::class.java.`package`.implementationVersion ?: "development"
    }
}

fun main(args: Array<String>) = KafkaCsvLoaderCommand().main(args)
