package com.dragos.kafkacsvloader.avro

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.Schema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class AvroSchemaLoaderTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun `should load valid avro schema`() {
        // Given
        val schemaContent = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
        """.trimIndent()
        val schemaFile = File(tempDir, "user.avsc").apply {
            writeText(schemaContent)
        }

        // When
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

        // Then
        schema shouldNotBe null
        schema.type shouldBe Schema.Type.RECORD
        schema.name shouldBe "User"
        schema.namespace shouldBe "com.test"
        schema.fields.size shouldBe 2
    }

    @Test
    fun `should throw exception when file does not exist`() {
        // Given
        val nonExistentPath = "/path/to/nonexistent/schema.avsc"

        // When/Then
        val exception = shouldThrow<IllegalArgumentException> {
            AvroSchemaLoader.loadFromFile(nonExistentPath)
        }
        exception.message shouldContain "not found"
    }

    @Test
    fun `should throw exception when file is not readable`() {
        // Given
        val schemaFile = File(tempDir, "unreadable.avsc").apply {
            writeText("{}")
            setReadable(false)
        }

        // When/Then
        try {
            val exception = shouldThrow<IllegalArgumentException> {
                AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)
            }
            exception.message shouldContain "not readable"
        } finally {
            schemaFile.setReadable(true) // cleanup
        }
    }

    @Test
    fun `should throw exception for invalid schema syntax`() {
        // Given
        val invalidSchemaContent = """
            {
              "type": "record",
              "name": "User",
              "fields": "THIS IS INVALID"
            }
        """.trimIndent()
        val schemaFile = File(tempDir, "invalid.avsc").apply {
            writeText(invalidSchemaContent)
        }

        // When/Then
        shouldThrow<Exception> {
            AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)
        }
    }

    @Test
    fun `should handle schema with nullable fields`() {
        // Given
        val schemaContent = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": ["null", "string"], "default": null}
              ]
            }
        """.trimIndent()
        val schemaFile = File(tempDir, "user_nullable.avsc").apply {
            writeText(schemaContent)
        }

        // When
        val schema = AvroSchemaLoader.loadFromFile(schemaFile.absolutePath)

        // Then
        schema.fields.size shouldBe 2
        val emailField = schema.getField("email")
        emailField.schema().type shouldBe Schema.Type.UNION
    }
}