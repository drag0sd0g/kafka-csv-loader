package com.dragos.kafkacsvloader.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.mordant.rendering.TextColors.*
import com.github.ajalt.mordant.terminal.Terminal

class LoadCommand : CliktCommand(
    name = "load",
    help = "Load CSV data into Kafka with Avro schema validation"
) {
    private val csvFile by option("--csv", "-c", help = "Path to CSV file").required()
    private val topic by option("--topic", "-t", help = "Kafka topic name").required()
    private val schemaFile by option("--schema", "-s", help = "Path to Avro schema file").required()
    private val bootstrapServers by option("--bootstrap-servers", "-b", help = "Kafka bootstrap servers")
        .default("localhost:9092")
    private val schemaRegistry by option("--schema-registry", "-r", help = "Schema Registry URL")
        .default("http://localhost:8081")
    private val dryRun by option("--dry-run", help = "Validate only, don't produce to Kafka").flag()
    
    private val terminal = Terminal()

    override fun run() {
        terminal.println(green("🚀 Kafka CSV Loader"))
        terminal.println()
        terminal.println("Configuration:")
        terminal.println("  CSV File: $csvFile")
        terminal.println("  Topic: $topic")
        terminal.println("  Schema: $schemaFile")
        terminal.println("  Bootstrap Servers: $bootstrapServers")
        terminal.println("  Schema Registry: $schemaRegistry")
        terminal.println("  Dry Run: $dryRun")
        terminal.println()
        
        if (dryRun) {
            terminal.println(yellow("⚠️  Dry run mode - no messages will be produced"))
        }
        
        terminal.println(red("❌ Not implemented yet - this is just the CLI scaffold"))
        terminal.println()
        terminal.println(blue("Next steps:"))
        terminal.println("  1. Implement CSV parsing")
        terminal.println("  2. Implement Avro schema loading")
        terminal.println("  3. Implement Kafka producer")
        terminal.println("  4. Add validation logic")
    }
}