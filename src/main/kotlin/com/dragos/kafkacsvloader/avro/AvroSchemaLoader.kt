package com.dragos.kafkacsvloader.avro

import org.apache.avro.Schema
import java.io.File

/**
 * Simple helper to load an Avro schema from a .avsc file.
 * Throws IllegalArgumentException if file is missing or unreadable,
 * and will propagate Parser exceptions if the schema text is invalid.
 */
object AvroSchemaLoader {
    fun loadFromFile(path: String): Schema {
        val file = File(path)
        require(file.exists() && file.isFile && file.canRead()) {
            "Avro schema file not found or not readable: $path"
        }
        val text = file.readText()
        return Schema.Parser().parse(text)
    }
}
