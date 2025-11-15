# 🚀 Kafka CSV Loader

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9.22-blue.svg)](https://kotlinlang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![JaCoCo](https://img.shields.io/badge/Coverage-80%25+-success.svg)](build/reports/jacoco/test/html/index.html)

A robust, production-ready Kotlin CLI tool for loading CSV data into Apache Kafka with Avro schema validation, Schema Registry integration, and configurable batching.

## 📋 Overview

Kafka CSV Loader bridges the gap between traditional CSV data formats and modern event streaming platforms. It provides a seamless, type-safe way to migrate CSV data into Kafka topics with full schema validation and registry integration.

**Use Cases:**

-   **Data Migration**: Moving legacy CSV data into Kafka-based systems
-   **Batch Loading**: Periodic bulk imports from CSV exports with configurable batching
-   **Data Integration**: Connecting CSV-based systems to event-driven architectures
-   **Testing & Development**: Quickly populating Kafka topics with test data
-   **Data Validation**: Dry-run mode to validate CSV data before production loads

## ✨ Features

✅ **CSV Parsing** - Intelligent CSV parsing with header validation  
✅ **Avro Schema Validation** - Type-safe data validation against Avro schemas  
✅ **Schema Registry Integration** - Automatic schema registration and versioning  
✅ **Dry Run Mode** - Validate CSV and schema without sending to Kafka  
✅ **Configurable Batching** - Batch records for improved performance  
✅ **Async/Sync Modes** - Choose between sync (safe) or async (fast) sending  
✅ **Error Handling** - Detailed validation errors with row-level reporting  
✅ **Flexible Key Selection** - Choose any CSV column as Kafka message key  
✅ **Colorful CLI** - Beautiful terminal output with progress indicators  
✅ **Production Ready** - 80%+ test coverage with unit and integration tests  
✅ **Code Quality** - Ktlint formatting, JaCoCo coverage reporting

## 🏗️ Architecture

```
┌─────────────────┐
│   CSV File      │
│  (users.csv)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CSV Parser     │  ← Validates headers
│  (kotlin-csv)   │    Parses rows
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Avro Schema     │  ← Loads .avsc file
│ Loader          │    Validates structure
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Avro Record     │  ← Maps CSV → Avro
│ Mapper          │    Type conversion
└────────┬────────┘    Validation
         │
         ▼
┌─────────────────┐
│ Dry Run Mode?   │  ← Optional validation
│                 │    (skip Kafka)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Batching        │  ← Configurable batch size
│ (optional)      │    Sync or Async mode
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Producer  │  ← Sends to Kafka
│ (Avro Serial.)  │    Schema Registry
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Topic    │
│ (with Schema)   │
└─────────────────┘
```

## 🛠️ Technologies

-   **Language**: Kotlin 1.9.22 (JVM 21)
-   **Build Tool**: Gradle 8.14 with Kotlin DSL
-   **CLI Framework**: Clikt 4.2.1 (command-line parsing)
-   **Terminal UI**: Mordant 2.2.0 (colored output, progress indicators)
-   **CSV Parsing**: kotlin-csv-jvm 1.9.2
-   **Avro**: Apache Avro 1.11.3
-   **Kafka**: kafka-clients 3.6.1
-   **Schema Registry**: Confluent Schema Registry 7.5.3
-   **Testing**: JUnit 5, Kotest, Testcontainers, Mockk
-   **Code Quality**: Ktlint 1.0.1, JaCoCo 0.8.11
-   **Containerization**: Docker/Colima support with Testcontainers

## 📦 Installation

### Prerequisites

-   **Java 21+** (JDK)
-   **Docker or Colima** (for running Kafka locally or integration tests)
-   **Kafka & Schema Registry** (running instances for production use)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/drag0sd0g/kafka-csv-loader.git
cd kafka-csv-loader

# Build the project (includes tests, code coverage, linting)
./gradlew build

# Build fat JAR
./gradlew jar

# The executable JAR will be at:
# build/libs/kafka-csv-loader-*.jar
```

### Run Tests

```bash
# Run all tests
./gradlew test

# Run tests with coverage report
./gradlew test jacocoTestReport

# View coverage report
open build/reports/jacoco/test/html/index.html

# Run only unit tests
./gradlew test --tests "*.csv.*" --tests "*.avro.*"

# Run integration tests (requires Docker/Colima)
./gradlew test --tests "*IntegrationTest"
```

## 🚀 Quick Start

### 1. Prepare Your Data

**Example CSV** (`users.csv`):

```csv
id,name,email,age,active
1,Alice,alice@example.com,30,true
2,Bob,bob@example.com,25,false
3,Charlie,charlie@example.com,35,true
```

**Example Avro Schema** (`user-schema.avsc`):

```json
{
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        { "name": "id", "type": "int" },
        { "name": "name", "type": "string" },
        { "name": "email", "type": "string" },
        { "name": "age", "type": "int" },
        { "name": "active", "type": "boolean" }
    ]
}
```

### 2. Start Kafka & Schema Registry

```bash
# Using Docker Compose (example)
docker-compose up -d kafka schema-registry

# Or using Confluent Platform
confluent local services start
```

### 3. Validate Data (Dry Run)

Before loading to production, validate your CSV:

```bash
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --dry-run
```

### 4. Load Data to Kafka

```bash
# Basic loading (row-by-row)
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --bootstrap-servers localhost:9092 \
  --schema-registry http://localhost:8081 \
  --key-field id

# With batching for better performance
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --batch-size 100
```

### 5. Verify Data in Kafka

```bash
# Using kafka-avro-console-consumer
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users \
  --from-beginning
```

## 📖 Usage

### Command-Line Options

```
Usage: kafka-csv-loader [OPTIONS]

  Load CSV data into Kafka with Avro schema validation

Options:
  -c, --csv TEXT              Path to CSV file (required)
  -s, --schema TEXT           Path to Avro schema file (.avsc) (required)
  -t, --topic TEXT            Kafka topic name (required)
  -b, --bootstrap-servers     Kafka bootstrap servers (default: localhost:9092)
  -r, --schema-registry       Schema Registry URL (default: http://localhost:8081)
  -k, --key-field TEXT        CSV column to use as Kafka message key (optional)
  -d, --dry-run               Validate CSV and schema without sending to Kafka
  --batch-size INT            Number of records to batch (default: 1 = no batching)
  --async                     Send batches asynchronously (faster but less safe)
  --version                   Show version and exit
  -h, --help                  Show this message and exit
```

### Examples

#### Basic Usage (Row-by-Row)

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic
```

#### With Custom Kafka Configuration

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic \
  --bootstrap-servers kafka1:9092,kafka2:9092 \
  --schema-registry http://schema-registry:8081
```

#### Using a Specific Column as Message Key

```bash
java -jar kafka-csv-loader.jar \
  --csv orders.csv \
  --schema order-schema.avsc \
  --topic orders \
  --key-field order_id
```

#### With Batching (Recommended for Large Files)

```bash
# Synchronous batching (safe, recommended)
java -jar kafka-csv-loader.jar \
  --csv large-file.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100

# Asynchronous batching (maximum performance)
java -jar kafka-csv-loader.jar \
  --csv large-file.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100 \
  --async
```

#### Dry Run Mode (Validation Only)

```bash
java -jar kafka-csv-loader.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --dry-run
```

## 🔍 Dry Run Mode

Validate your CSV and schema without actually sending data to Kafka using the `--dry-run` flag.

**What it does:**

-   ✅ Loads and validates the Avro schema
-   ✅ Parses the CSV file
-   ✅ Validates CSV headers match schema fields
-   ✅ Validates all rows can be mapped to Avro records
-   ✅ Reports validation errors with row numbers
-   ❌ **Does NOT** connect to Kafka
-   ❌ **Does NOT** send any data

**Use cases:**

-   Test your CSV data before loading to production
-   Validate schema compatibility
-   Find data quality issues early
-   CI/CD pipeline validation

**Example output:**

```
🚀 Kafka CSV Loader
   DRY RUN MODE - No data will be sent to Kafka

📋 Loading Avro schema... ✓
   Schema: com.example.User
   Fields: id, name, email, age

📄 Parsing CSV file... ✓
   Headers: id, name, email, age
   Rows: 1000

🔍 Validating CSV headers against schema... ✓

🔍 Validating all rows (dry run)...
   ✓ Validated 50 rows...
   ✓ Validated 100 rows...
   ...

📊 Dry Run Summary
   ✓ Valid rows: 1000
   ✗ Invalid rows: 0

✅ All rows validated successfully! Ready to load to Kafka.
```

## ⚡ Batching & Performance

For large CSV files, batching can significantly improve performance by reducing network roundtrips and improving throughput.

### Batch Options

-   `--batch-size N` - Number of records to batch before sending (default: 1 = no batching)
-   `--async` - Send batches asynchronously (faster, but requires monitoring)

### Performance Comparison

| Mode        | Batch Size | 1K rows | 10K rows | 100K rows | Notes                        |
| ----------- | ---------- | ------- | -------- | --------- | ---------------------------- |
| Row-by-row  | 1          | ~3s     | ~30s     | ~5min     | Slowest, most reliable       |
| Sync batch  | 50         | ~1s     | ~10s     | ~100s     | Good balance                 |
| Sync batch  | 100        | ~0.8s   | ~8s      | ~80s      | Recommended for production   |
| Async batch | 100        | ~0.5s   | ~5s      | ~50s      | Fastest, requires monitoring |

### Batching Examples

#### Small Files (<1K rows)

Use default (no batching):

```bash
java -jar kafka-csv-loader.jar \
  --csv small.csv \
  --schema schema.avsc \
  --topic my-topic
```

#### Medium Files (1K-10K rows)

Use sync batching with batch size 50:

```bash
java -jar kafka-csv-loader.jar \
  --csv medium.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 50
```

#### Large Files (>10K rows)

Use sync batching with batch size 100:

```bash
java -jar kafka-csv-loader.jar \
  --csv large.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100
```

#### Maximum Performance (async)

Use async batching for maximum throughput:

```bash
java -jar kafka-csv-loader.jar \
  --csv huge.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100 \
  --async
```

### Recommendations

-   **Development/Testing**: Use default (no batching) for easier debugging
-   **Small files (<1K rows)**: Use default (no batching)
-   **Medium files (1K-10K rows)**: Use `--batch-size 50`
-   **Large files (>10K rows)**: Use `--batch-size 100`
-   **Production**: Always start with sync batching, test thoroughly before using async
-   **Async mode**: Only use after testing; monitor for errors carefully

### Batching Output Example

```
🚀 Kafka CSV Loader

📋 Loading Avro schema... ✓
   Schema: com.example.User
   Fields: id, name, email, age

📄 Parsing CSV file... ✓
   Headers: id, name, email, age
   Rows: 10000

🔍 Validating CSV headers against schema... ✓

🔌 Connecting to Kafka...
   Bootstrap servers: localhost:9092
   Schema Registry: http://localhost:8081
   Topic: users

📤 Sending records to Kafka...
   Batch size: 100, Mode: sync

   ✓ Processed 50 rows...
   ✓ Processed 100 rows...
   ...
   ✓ Processed 10000 rows...


📊 Summary
   ✓ Success: 10000
   ✗ Failures: 0

✅ All records successfully loaded!
```

## 🏭 Project Structure

```
kafka-csv-loader/
├── src/
│   ├── main/kotlin/com/dragos/kafkacsvloader/
│   │   ├── cli/
│   │   │   └── LoadCommand.kt          # CLI entry point & command handler
│   │   ├── csv/
│   │   │   └── CsvParser.kt            # CSV parsing and validation
│   │   ├── avro/
│   │   │   ├── AvroSchemaLoader.kt     # Schema loading from .avsc files
│   │   │   └── AvroRecordMapper.kt     # CSV → Avro mapping & type conversion
│   │   └── kafka/
│   │       └── KafkaProducerClient.kt  # Kafka producer with batching support
│   └── test/kotlin/com/dragos/kafkacsvloader/
│       ├── cli/
│       │   └── DryRunTest.kt           # Dry-run mode tests
│       ├── csv/
│       │   └── CsvParserTest.kt        # CSV parsing tests
│       ├── avro/
│       │   ├── AvroSchemaLoaderTest.kt # Schema loading tests
│       │   └── AvroRecordMapperTest.kt # Avro mapping tests
│       ├── kafka/
│       │   └── KafkaProducerBatchTest.kt # Batching tests
│       └── integration/
│           └── KafkaIntegrationTest.kt # End-to-end Testcontainers tests
├── build.gradle.kts                     # Build configuration with plugins
├── .github/
│   └── workflows/
│       └── release.yml                  # CI/CD and release automation
├── .axion.yml                           # Semantic versioning configuration
└── README.md
```

## 🐛 Error Handling

The tool provides detailed error reporting at every stage:

### Schema Validation Errors

```
❌ Error: Schema validation failed
   Row 5: Field 'age' - Type conversion error: Cannot convert 'invalid' to int
   Row 7: Field 'email' - Missing value for required field
```

### Missing CSV Headers

```
❌ Error: CSV validation failed
   Missing required fields: age, email
```

### Kafka Connection Errors

```
❌ Error: Failed to connect to Kafka
   Caused by: Connection refused: localhost:9092
```

### Batch Send Errors

```
📊 Summary
   ✓ Success: 9950
   ✗ Failures: 50

   Invalid rows:
     Row 100: Kafka batch error: Timeout waiting for acknowledgment
     Row 200: Kafka batch error: Timeout waiting for acknowledgment
     ...
```

### Dry Run Validation Errors

```
📊 Dry Run Summary
   ✓ Valid rows: 998
   ✗ Invalid rows: 2

   Invalid rows:
     Row 5: Field 'age' conversion error: For input string: "invalid"
     Row 42: Missing value for required field 'email'
```

## 🧪 Testing

### Test Coverage

The project maintains high test coverage with multiple test types:

-   ✅ **Unit Tests**: CSV parsing, Avro mapping, validation logic, batching
-   ✅ **Integration Tests**: End-to-end with Testcontainers (Kafka + Schema Registry)
-   ✅ **CLI Tests**: Dry-run mode validation
-   📊 **Coverage**: 80%+ code coverage (measured by JaCoCo)

### Running Tests

```bash
# All tests
./gradlew test

# Unit tests only
./gradlew test --tests "*.csv.*" --tests "*.avro.*"

# Integration tests (requires Docker/Colima)
./gradlew test --tests "*IntegrationTest"

# Batching tests
./gradlew test --tests "*BatchTest"

# Generate coverage report
./gradlew jacocoTestReport
open build/reports/jacoco/test/html/index.html
```

### Code Quality

```bash
# Run ktlint checks
./gradlew ktlintCheck

# Auto-format code
./gradlew ktlintFormat

# Full quality check (lint + coverage)
./gradlew check
```

## 🔧 Configuration for Colima (macOS)

If you're using Colima instead of Docker Desktop for integration tests:

```bash
# Start Colima
colima start

# Set environment variables
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$HOME/.colima/default/docker.sock"

# Add to ~/.zshrc for persistence
echo 'export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"' >> ~/.zshrc
echo 'export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$HOME/.colima/default/docker.sock"' >> ~/.zshrc
```

## 📦 Releases

This project uses semantic versioning with automatic releases on every commit to `main`:

-   **Format**: `v0.0.1`, `v0.0.2`, etc.
-   **Automation**: GitHub Actions automatically tags and creates releases
-   **Artifacts**: JAR files are attached to each release

View releases: [https://github.com/drag0sd0g/kafka-csv-loader/releases](https://github.com/drag0sd0g/kafka-csv-loader/releases)

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests and linting (`./gradlew build`)
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

**Code Standards:**

-   Follow Kotlin coding conventions
-   Maintain 80%+ test coverage
-   Pass all ktlint checks
-   Add tests for new features

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

-   Built with [Clikt](https://ajalt.github.io/clikt/) for elegant CLI parsing
-   Terminal UI powered by [Mordant](https://github.com/ajalt/mordant)
-   CSV parsing by [kotlin-csv](https://github.com/doyaaaaaken/kotlin-csv)
-   Integration testing with [Testcontainers](https://www.testcontainers.org/)
-   Code quality with [Ktlint](https://ktlint.github.io/)
-   Coverage reporting with [JaCoCo](https://www.jacoco.org/)

## 📧 Contact

**Dragos** - [@drag0sd0g](https://github.com/drag0sd0g)

Project Link: [https://github.com/drag0sd0g/kafka-csv-loader](https://github.com/drag0sd0g/kafka-csv-loader)

---

Made with ❤️ and ☕ by Dragos
