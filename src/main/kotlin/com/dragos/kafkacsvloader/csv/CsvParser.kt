package com.dragos.kafkacsvloader.csv

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import java.io.File

/**
 * CSV data structure: headers and rows as maps.
 */
data class CsvData(
    val headers: List<String>,
    val rows: List<Map<String, String>>
)

/**
 * CSV parser that reads CSV files and returns structured data.
 */
object CsvParser {
    
    /**
     * Parse a CSV file and return headers + rows.
     * Each row is represented as a Map<columnName, cellValue>.
     * Validates file existence, extension, and readability.
     */
    fun parse(filePath: String): CsvData {
        val file = File(filePath)
        
        // Validate file
        require(file.exists()) { "CSV file not found: $filePath" }
        require(file.isFile) { "Path is not a file: $filePath" }
        require(file.canRead()) { "CSV file is not readable: $filePath" }
        require(file.extension.equals("csv", ignoreCase = true)) { 
            "File must have .csv extension: $filePath" 
        }
        
        // Parse CSV using kotlin-csv library
        val allRows = csvReader().readAllWithHeader(file)
        
        require(allRows.isNotEmpty()) { "CSV file is empty or has no data rows: $filePath" }
        
        val headers = allRows.first().keys.toList()
        require(headers.isNotEmpty()) { "CSV file has no headers: $filePath" }
        
        return CsvData(
            headers = headers,
            rows = allRows
        )
    }
    
    /**
     * Validate that all required schema fields are present in CSV headers.
     * Returns true if all schema fields exist in CSV headers.
     */
    fun validateHeaders(csvHeaders: List<String>, schemaFields: List<String>): Boolean {
        return schemaFields.all { it in csvHeaders }
    }
    
    /**
     * Get list of schema fields that are missing from CSV headers.
     */
    fun getMissingFields(csvHeaders: List<String>, schemaFields: List<String>): List<String> {
        return schemaFields.filter { it !in csvHeaders }
    }
}