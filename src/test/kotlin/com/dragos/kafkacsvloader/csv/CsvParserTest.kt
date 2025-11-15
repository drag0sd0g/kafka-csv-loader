package com.dragos.kafkacsvloader.csv

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class CsvParserTest {
    @TempDir
    lateinit var tempDir: File

    @Test
    fun `should parse valid CSV file`() {
        // Given
        val csvContent =
            """
            id,name,email,age
            1,Alice,alice@example.com,30
            2,Bob,bob@example.com,25
            """.trimIndent()
        val csvFile =
            File(tempDir, "users.csv").apply {
                writeText(csvContent)
            }

        // When
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // Then
        csvData.headers shouldContainExactly listOf("id", "name", "email", "age")
        csvData.rows.size shouldBe 2
        csvData.rows[0]["id"] shouldBe "1"
        csvData.rows[0]["name"] shouldBe "Alice"
        csvData.rows[0]["email"] shouldBe "alice@example.com"
        csvData.rows[0]["age"] shouldBe "30"
        csvData.rows[1]["id"] shouldBe "2"
        csvData.rows[1]["name"] shouldBe "Bob"
    }

    @Test
    fun `should throw exception when file does not exist`() {
        // When/Then
        val exception =
            shouldThrow<IllegalArgumentException> {
                CsvParser.parse("/nonexistent/file.csv")
            }
        exception.message shouldContain "not found"
    }

    @Test
    fun `should throw exception when file is not a CSV`() {
        // Given
        val txtFile =
            File(tempDir, "data.txt").apply {
                writeText("some text")
            }

        // When/Then
        val exception =
            shouldThrow<IllegalArgumentException> {
                CsvParser.parse(txtFile.absolutePath)
            }
        exception.message shouldContain ".csv extension"
    }

    @Test
    fun `should throw exception when CSV is empty`() {
        // Given
        val emptyFile =
            File(tempDir, "empty.csv").apply {
                writeText("")
            }

        // When/Then
        shouldThrow<IllegalArgumentException> {
            CsvParser.parse(emptyFile.absolutePath)
        }
    }

    @Test
    fun `should throw exception when CSV has headers but no data rows`() {
        // Given
        val headerOnlyFile =
            File(tempDir, "headers_only.csv").apply {
                writeText("id,name,email")
            }

        // When/Then
        val exception =
            shouldThrow<IllegalArgumentException> {
                CsvParser.parse(headerOnlyFile.absolutePath)
            }
        exception.message shouldContain "empty"
    }

    @Test
    fun `should handle CSV with quoted values`() {
        // Given
        val csvContent =
            """
            id,name,description
            1,"Alice","A user with, comma"
            2,"Bob","Another ""quoted"" value"
            """.trimIndent()
        val csvFile =
            File(tempDir, "quoted.csv").apply {
                writeText(csvContent)
            }

        // When
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // Then
        csvData.rows.size shouldBe 2
        csvData.rows[0]["description"] shouldBe "A user with, comma"
        csvData.rows[1]["description"] shouldBe "Another \"quoted\" value"
    }

    @Test
    fun `validateHeaders should return true when all schema fields present`() {
        // Given
        val csvHeaders = listOf("id", "name", "email", "age")
        val schemaFields = listOf("id", "name", "email")

        // When
        val result = CsvParser.validateHeaders(csvHeaders, schemaFields)

        // Then
        result shouldBe true
    }

    @Test
    fun `validateHeaders should return false when schema field is missing`() {
        // Given
        val csvHeaders = listOf("id", "name")
        val schemaFields = listOf("id", "name", "email", "age")

        // When
        val result = CsvParser.validateHeaders(csvHeaders, schemaFields)

        // Then
        result shouldBe false
    }

    @Test
    fun `getMissingFields should return missing schema fields`() {
        // Given
        val csvHeaders = listOf("id", "name")
        val schemaFields = listOf("id", "name", "email", "age")

        // When
        val missing = CsvParser.getMissingFields(csvHeaders, schemaFields)

        // Then
        missing shouldContainExactly listOf("email", "age")
    }

    @Test
    fun `getMissingFields should return empty list when all fields present`() {
        // Given
        val csvHeaders = listOf("id", "name", "email", "age")
        val schemaFields = listOf("id", "name")

        // When
        val missing = CsvParser.getMissingFields(csvHeaders, schemaFields)

        // Then
        missing shouldBe emptyList()
    }

    @Test
    fun `should handle CSV with extra columns not in schema`() {
        // Given
        val csvContent =
            """
            id,name,email,extra_column
            1,Alice,alice@example.com,extra_value
            """.trimIndent()
        val csvFile =
            File(tempDir, "extra_cols.csv").apply {
                writeText(csvContent)
            }

        // When
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // Then
        csvData.headers shouldContain "extra_column"
        csvData.rows[0]["extra_column"] shouldBe "extra_value"
    }

    @Test
    fun `should handle CSV with empty cells`() {
        // Given
        val csvContent =
            """
            id,name,email
            1,Alice,
            2,,bob@example.com
            """.trimIndent()
        val csvFile =
            File(tempDir, "empty_cells.csv").apply {
                writeText(csvContent)
            }

        // When
        val csvData = CsvParser.parse(csvFile.absolutePath)

        // Then
        csvData.rows[0]["email"] shouldBe ""
        csvData.rows[1]["name"] shouldBe ""
    }
}
