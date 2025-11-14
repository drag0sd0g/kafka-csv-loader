# 🚀 Kafka CSV Loader

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9.22-blue.svg)](https://kotlinlang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-Passing-success.svg)](https://github.com/drag0sd0g/kafka-csv-loader)

A robust, production-ready Kotlin CLI tool for loading CSV data into Apache Kafka with Avro schema validation and Schema Registry integration.

## 📋 Overview

Kafka CSV Loader bridges the gap between traditional CSV data formats and modern event streaming platforms. It provides a seamless, type-safe way to migrate CSV data into Kafka topics with full schema validation, making it ideal for:

- **Data Migration**: Moving legacy CSV data into Kafka-based systems
- **Batch Loading**: Periodic bulk imports from CSV exports
- **Data Integration**: Connecting CSV-based systems to event-driven architectures
- **Testing & Development**: Quickly populating Kafka topics with test data

## ✨ Features

✅ **CSV Parsing** - Intelligent CSV parsing with header validation  
✅ **Avro Schema Validation** - Type-safe data validation against Avro schemas  
✅ **Schema Registry Integration** - Automatic schema registration and versioning  
✅ **Batch Processing** - Efficient bulk loading with progress tracking  
✅ **Error Handling** - Detailed validation errors with row-level reporting  
✅ **Flexible Key Selection** - Choose any CSV column as Kafka message key  
✅ **Colorful CLI** - Beautiful terminal output with progress indicators  
✅ **Production Ready** - Comprehensive test coverage with integration tests

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
│ Kafka Producer  │  ← Sends to Kafka
│ (Avro Serial.)  │    Schema Registry
└────────┬────────┘    Sync/Async
         │
         ▼
┌─────────────────┐
│  Kafka Topic    │
│ (with Schema)   │
└─────────────────┘
```

## 🛠️ Technologies

- **Language**: Kotlin 1.9.22 (JVM 21)
- **Build Tool**: Gradle 8.5 with Kotlin DSL
- **CLI Framework**: Clikt 4.2.1 (command-line parsing)
- **Terminal UI**: Mordant 2.2.0 (colored output, progress bars)
- **CSV Parsing**: kotlin-csv-jvm 1.9.2
- **Avro**: Apache Avro 1.11.3
- **Kafka**: kafka-clients 3.6.1
- **Schema Registry**: Confluent Schema Registry 7.5.3
- **Testing**: JUnit 5, Kotest, Testcontainers
- **Containerization**: Docker/Colima support

## 📦 Installation

### Prerequisites

- **Java 21+** (JDK)
- **Docker or Colima** (for running Kafka locally)
- **Kafka & Schema Registry** (running instances)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/drag0sd0g/kafka-csv-loader.git
cd kafka-csv-loader

# Build the project
./gradlew build

# Build fat JAR
./gradlew jar

# The executable JAR will be at:
# build/libs/kafka-csv-loader-0.1.0-SNAPSHOT.jar
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

### 3. Load Data

```bash
java -jar build/libs/kafka-csv-loader-0.1.0-SNAPSHOT.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --bootstrap-servers localhost:9092 \
  --schema-registry http://localhost:8081 \
  --key-field id
```

### 4. Verify Data in Kafka

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
  -h, --help                  Show this message and exit
```

### Examples

**Basic usage:**

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic
```

**With custom Kafka configuration:**

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic \
  --bootstrap-servers kafka1:9092,kafka2:9092 \
  --schema-registry http://schema-registry:8081
```

**Using a specific column as message key:**

```bash
java -jar kafka-csv-loader.jar \
  --csv orders.csv \
  --schema order-schema.avsc \
  --topic orders \
  --key-field order_id
```

## 🧪 Testing

### Run All Tests

```bash
./gradlew test
```

### Run Unit Tests Only

```bash
./gradlew test --tests "*.csv.*" --tests "*.avro.*"
```

### Run Integration Tests

**Note**: Integration tests require Docker/Colima to be running.

```bash
# Make sure Docker/Colima is running
colima status  # or: docker ps

# Run integration tests
./gradlew test --tests "KafkaIntegrationTest"
```

### Test Coverage

- ✅ **Unit Tests**: CSV parsing, Avro mapping, validation logic
- ✅ **Integration Tests**: End-to-end with Testcontainers (Kafka + Schema Registry)
- 📊 **Coverage**: 85%+ code coverage

## 🏭 Project Structure

```
kafka-csv-loader/
├── src/
│   ├── main/kotlin/com/dragos/kafkacsvloader/
│   │   ├── Main.kt                    # CLI entry point
│   │   ├── csv/
│   │   │   ├── CsvParser.kt           # CSV parsing logic
│   │   │   └── CsvData.kt             # Data models
│   │   ├── avro/
│   │   │   ├── AvroSchemaLoader.kt    # Schema loading
│   │   │   ├── AvroRecordMapper.kt    # CSV → Avro mapping
│   │   │   └── RowMappingResult.kt    # Result types
│   │   └── kafka/
│   │       └── KafkaProducerClient.kt # Kafka producer
│   └── test/
│       ├── kotlin/com/dragos/kafkacsvloader/
│       │   ├── csv/CsvParserTest.kt
│       │   ├── avro/AvroRecordMapperTest.kt
│       │   └── integration/KafkaIntegrationTest.kt
│       └── resources/
│           └── integration/            # Test fixtures
├── build.gradle.kts                    # Build configuration
└── README.md
```

## 🐛 Error Handling

The tool provides detailed error reporting:

**Schema Validation Errors:**

```
❌ Error: Schema validation failed
   Row 5: Field 'age' - Type conversion error: Cannot convert 'invalid' to int
   Row 7: Field 'email' - Missing value for required field
```

**Missing CSV Headers:**

```
❌ Error: CSV validation failed
   Missing required fields: age, email
```

**Kafka Connection Errors:**

```
❌ Error: Failed to connect to Kafka
   Caused by: Connection refused: localhost:9092
```

## 🔧 Configuration for Colima (macOS)

If you're using Colima instead of Docker Desktop:

```bash
# Set environment variables
export DOCKER_HOST="unix:///Users/$USER/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/Users/$USER/.colima/default/docker.sock"

# Add to ~/.zshrc for persistence
echo 'export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"' >> ~/.zshrc
echo 'export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="$HOME/.colima/default/docker.sock"' >> ~/.zshrc
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Clikt](https://ajalt.github.io/clikt/) for CLI parsing
- Terminal UI powered by [Mordant](https://github.com/ajalt/mordant)
- CSV parsing by [kotlin-csv](https://github.com/doyaaaaaken/kotlin-csv)
- Integration testing with [Testcontainers](https://www.testcontainers.org/)

## 📧 Contact

**Dragos** - [@drag0sd0g](https://github.com/drag0sd0g)

Project Link: [https://github.com/drag0sd0g/kafka-csv-loader](https://github.com/drag0sd0g/kafka-csv-loader)

---

Made with ❤️ and ☕ by Dragos
