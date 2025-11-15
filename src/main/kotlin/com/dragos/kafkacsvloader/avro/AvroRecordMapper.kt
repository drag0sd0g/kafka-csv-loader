package com.dragos.kafkacsvloader.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

sealed interface RowMappingResult {
    data class Success(val record: GenericRecord) : RowMappingResult

    data class Failure(val errors: List<String>) : RowMappingResult
}

/**
 * Maps CSV row (Map<columnName, stringValue>) to Avro GenericRecord.
 * Handles primitive types: null, string, int, long, boolean, float, double.
 * TODO: extend to handle records, enums, arrays, maps, complex unions.
 */
object AvroRecordMapper {
    fun mapRow(
        schema: Schema,
        row: Map<String, String?>,
    ): RowMappingResult {
        val record = GenericData.Record(schema)
        val errors = mutableListOf<String>()

        for (field in schema.fields) {
            val fieldName = field.name()
            val fieldSchema = field.schema()
            val resolvedSchema = resolveSchema(fieldSchema)

            val rawValue = row[fieldName]?.trim()

            // Handle missing or empty values
            if (rawValue.isNullOrEmpty()) {
                when {
                    field.hasDefaultValue() -> {
                        // Let Avro use the default value
                        continue
                    }
                    isNullable(fieldSchema) -> {
                        record.put(fieldName, null)
                    }
                    else -> {
                        errors += "Missing value for required field '$fieldName'"
                    }
                }
                continue
            }

            // Convert the string value to the appropriate type
            try {
                val converted = convertValue(resolvedSchema, rawValue)
                record.put(fieldName, converted)
            } catch (e: Exception) {
                errors += "Field '$fieldName' conversion error: ${e.message}"
            }
        }

        return if (errors.isEmpty()) {
            RowMappingResult.Success(record)
        } else {
            RowMappingResult.Failure(errors)
        }
    }

    private fun resolveSchema(schema: Schema): Schema {
        return if (schema.type == Schema.Type.UNION) {
            // Find the non-null type in the union
            schema.types.find { it.type != Schema.Type.NULL } ?: schema.types.first()
        } else {
            schema
        }
    }

    private fun isNullable(schema: Schema): Boolean {
        return schema.type == Schema.Type.UNION &&
            schema.types.any { it.type == Schema.Type.NULL }
    }

    private fun convertValue(
        schema: Schema,
        raw: String,
    ): Any {
        return when (schema.type) {
            Schema.Type.STRING -> raw
            Schema.Type.INT -> raw.toInt()
            Schema.Type.LONG -> raw.toLong()
            Schema.Type.BOOLEAN -> raw.toBooleanStrict()
            Schema.Type.FLOAT -> raw.toFloat()
            Schema.Type.DOUBLE -> raw.toDouble()
            Schema.Type.ENUM -> {
                // Validate enum symbol
                require(schema.hasEnumSymbol(raw)) {
                    "Invalid enum value '$raw'. Valid values: ${schema.enumSymbols}"
                }
                GenericData.EnumSymbol(schema, raw)
            }
            else -> throw IllegalArgumentException("Unsupported schema type: ${schema.type}")
        }
    }
}
