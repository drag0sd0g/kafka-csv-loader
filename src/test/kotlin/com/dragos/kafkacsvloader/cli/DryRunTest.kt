package com.dragos.kafkacsvloader.cli

import com.dragos.kafkacsvloader.avro.AvroRecordMapper
import com.dragos.kafkacsvloader.avro.AvroSchemaLoader
import com.dragos.kafkacsvloader.avro.RowMappingResult
import com.dragos.kafkacsvloader.csv.CsvParser
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import java.io.File
import java.nio.file.Files

class DryRunTest : FunSpec({

    test("dry run validates CSV and schema without errors for valid data") {
        val tempDir = Files.createTempDirectory("dry-run-test").toFile()
        try {
            // Create test CSV
            val csvFile = File(tempDir, "test.csv")
            csvFile.writeText(
                """
                id,name,email,age
                1,Alice,alice@example.com,30
                2,Bob,bob@example.com,25
                """.trimIndent(),
            )

            // Create test schema
            val schemaFile = File(tempDir, "schema.avsc")
            schemaFile.writeText(
                """
                {
                  "type": "record",
                  "name": "User",
                  "namespace": "com.example",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """.trimIndent(),
            )

            // Load schema
            val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

            // Parse CSV
            val csvData = CsvParser.parse(csvFile.absolutePath)

            // Validate headers
            val schemaFields = schema.fields.map { it.name() }
            val headersValid = CsvParser.validateHeaders(csvData.headers, schemaFields)
            headersValid shouldBe true

            // Validate all rows can be mapped (dry run logic)
            val failures = mutableListOf<Pair<Int, String>>()
            var validCount = 0

            csvData.rows.forEachIndexed { index, row ->
                val rowNumber = index + 1
                when (val result = AvroRecordMapper.mapRow(schema, row)) {
                    is RowMappingResult.Success -> validCount++
                    is RowMappingResult.Failure -> {
                        failures.add(rowNumber to result.errors.joinToString("; "))
                    }
                }
            }

            // Assert no failures
            validCount shouldBe 2
            failures.shouldBeEmpty()
        } finally {
            tempDir.deleteRecursively()
        }
    }

    test("dry run detects invalid rows") {
        val tempDir = Files.createTempDirectory("dry-run-test").toFile()
        try {
            // Create test CSV with invalid data
            val csvFile = File(tempDir, "test.csv")
            csvFile.writeText(
                """
                id,name,email,age
                1,Alice,alice@example.com,30
                2,Bob,bob@example.com,invalid_age
                """.trimIndent(),
            )

            // Create test schema
            val schemaFile = File(tempDir, "schema.avsc")
            schemaFile.writeText(
                """
                {
                  "type": "record",
                  "name": "User",
                  "namespace": "com.example",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """.trimIndent(),
            )

            // Load schema
            val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

            // Parse CSV
            val csvData = CsvParser.parse(csvFile.absolutePath)

            // Validate all rows (dry run logic)
            val failures = mutableListOf<Pair<Int, String>>()
            var validCount = 0

            csvData.rows.forEachIndexed { index, row ->
                val rowNumber = index + 1
                when (val result = AvroRecordMapper.mapRow(schema, row)) {
                    is RowMappingResult.Success -> validCount++
                    is RowMappingResult.Failure -> {
                        failures.add(rowNumber to result.errors.joinToString("; "))
                    }
                }
            }

            // Assert there's one failure
            validCount shouldBe 1
            failures shouldHaveSize 1
            failures[0].first shouldBe 2
            failures[0].second shouldBe "Field 'age' conversion error: For input string: \"invalid_age\""
        } finally {
            tempDir.deleteRecursively()
        }
    }

    test("dry run detects missing required fields") {
        val tempDir = Files.createTempDirectory("dry-run-test").toFile()
        try {
            // Create test CSV missing 'age' column
            val csvFile = File(tempDir, "test.csv")
            csvFile.writeText(
                """
                id,name,email
                1,Alice,alice@example.com
                2,Bob,bob@example.com
                """.trimIndent(),
            )

            // Create test schema
            val schemaFile = File(tempDir, "schema.avsc")
            schemaFile.writeText(
                """
                {
                  "type": "record",
                  "name": "User",
                  "namespace": "com.example",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "age", "type": "int"}
                  ]
                }
                """.trimIndent(),
            )

            // Load schema
            val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

            // Parse CSV
            val csvData = CsvParser.parse(csvFile.absolutePath)

            // Validate headers
            val schemaFields = schema.fields.map { it.name() }
            val headersValid = CsvParser.validateHeaders(csvData.headers, schemaFields)

            // Should fail validation
            headersValid shouldBe false

            // Check missing fields
            val missingFields = CsvParser.getMissingFields(csvData.headers, schemaFields)
            missingFields shouldBe listOf("age")
        } finally {
            tempDir.deleteRecursively()
        }
    }
})
