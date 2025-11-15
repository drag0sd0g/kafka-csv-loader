package com.dragos.kafkacsvloader.avro

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.avro.Schema
import org.junit.jupiter.api.Test

class AvroRecordMapperTest {
    @Test
    fun `should map valid row with all primitive types`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "long"},
                {"name": "score", "type": "double"},
                {"name": "rating", "type": "float"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "42",
                "name" to "Alice",
                "age" to "30",
                "score" to "95.5",
                "rating" to "4.5",
                "active" to "true",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Success>()
        val record = (result as RowMappingResult.Success).record
        record.get("id") shouldBe 42
        record.get("name") shouldBe "Alice"
        record.get("age") shouldBe 30L
        record.get("score") shouldBe 95.5
        record.get("rating") shouldBe 4.5f
        record.get("active") shouldBe true
    }

    @Test
    fun `should handle nullable fields with null values`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": ["null", "string"], "default": null}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "42",
                "email" to "",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Success>()
        val record = (result as RowMappingResult.Success).record
        record.get("id") shouldBe 42
        record.get("email") shouldBe null
    }

    @Test
    fun `should handle nullable fields with values`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": ["null", "string"], "default": null}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "42",
                "email" to "alice@example.com",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Success>()
        val record = (result as RowMappingResult.Success).record
        record.get("email") shouldBe "alice@example.com"
    }

    @Test
    fun `should fail when required field is missing`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row = mapOf("id" to "42")

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Failure>()
        val errors = (result as RowMappingResult.Failure).errors
        errors.size shouldBe 1
        val firstError = errors.first()
        firstError shouldContain "name"
        firstError shouldContain "Missing value"
    }

    @Test
    fun `should fail when type conversion fails`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "age", "type": "int"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "42",
                "age" to "not-a-number",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Failure>()
        val errors = (result as RowMappingResult.Failure).errors
        errors.size shouldBe 1
        val firstError = errors.first()
        firstError shouldContain "age"
        firstError shouldContain "conversion error"
    }

    @Test
    fun `should fail with multiple errors for multiple invalid fields`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "age", "type": "int"},
                {"name": "score", "type": "double"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "not-an-int",
                "age" to "not-a-number",
                "score" to "invalid",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Failure>()
        val errors = (result as RowMappingResult.Failure).errors
        errors.size shouldBe 3
    }

    @Test
    fun `should handle boolean conversion`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "active", "type": "boolean"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)

        // When/Then - true
        val resultTrue = AvroRecordMapper.mapRow(schema, mapOf("active" to "true"))
        resultTrue.shouldBeInstanceOf<RowMappingResult.Success>()
        (resultTrue as RowMappingResult.Success).record.get("active") shouldBe true

        // When/Then - false
        val resultFalse = AvroRecordMapper.mapRow(schema, mapOf("active" to "false"))
        resultFalse.shouldBeInstanceOf<RowMappingResult.Success>()
        (resultFalse as RowMappingResult.Success).record.get("active") shouldBe false
    }

    @Test
    fun `should fail for invalid boolean values`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "active", "type": "boolean"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)

        // When
        val result = AvroRecordMapper.mapRow(schema, mapOf("active" to "yes"))

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Failure>()
    }

    @Test
    fun `should handle whitespace in values`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row =
            mapOf(
                "id" to "  42  ",
                "name" to "  Alice  ",
            )

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Success>()
        val record = (result as RowMappingResult.Success).record
        record.get("id") shouldBe 42
        record.get("name") shouldBe "Alice"
    }

    @Test
    fun `should handle enum types`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE", "PENDING"]}}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row = mapOf("status" to "ACTIVE")

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Success>()
        val record = (result as RowMappingResult.Success).record
        record.get("status").toString() shouldBe "ACTIVE"
    }

    @Test
    fun `should fail for invalid enum value`() {
        // Given
        val schemaText =
            """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}}
              ]
            }
            """.trimIndent()
        val schema = Schema.Parser().parse(schemaText)
        val row = mapOf("status" to "INVALID_STATUS")

        // When
        val result = AvroRecordMapper.mapRow(schema, row)

        // Then
        result.shouldBeInstanceOf<RowMappingResult.Failure>()
        val errors = (result as RowMappingResult.Failure).errors
        val firstError = errors.first()
        firstError shouldContain "status"
    }
}
