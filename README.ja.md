[English](README.md)

# 🚀 Kafka CSV ローダー

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9.22-blue.svg)](https://kotlinlang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![JaCoCo](https://img.shields.io/badge/Coverage-80%25+-success.svg)](build/reports/jacoco/test/html/index.html)

堅牢かつ本番対応の Kotlin 製 CLI ツール。CSV データを Avro スキーマ検証付きで Kafka へ投入。スキーマレジストリ対応。バッチ処理やドライラン検証も便利。

---

## 📋 概要

Kafka CSV Loader は、従来型 CSV とモダンなストリーミング基盤（Kafka）を簡単かつ型安全につなぐ CLI です。移行、バルクロード、アドホック検証、イベント構造間データ移動などに最適です。

**用途例:**

-   **データ移行:** レガシー CSV データを Kafka へ
-   **バッチ投入:** 定期 CSV エクスポートのバッチ取込
-   **システム連携:** CSV データ → イベントアーキテクチャへの橋渡し
-   **テスト生成:** トピックへのテストデータ即投入
-   **データバリデーション:** 本番投入前の検証用 Dry run

---

## ✨ 特長

✅ **CSV パース** - ヘッダ検証付きスマートパース  
✅ **Avro スキーマ検証** - 型安全マッピング/妥当性確認  
✅ **スキーマレジストリ統合** - スキーマ自動登録・バージョニング  
✅ **Dry Run モード** - Kafka 送信せず検証だけ  
✅ **バッチ処理対応** - パフォーマンス改善&設定可  
✅ **同期/非同期送信** - 堅牢さ重視/速度優先の選択  
✅ **エラーハンドリング** - 行単位の詳細レポート  
✅ **柔軟な Key 設定** - 任意 CSV 列を Kafka キー化  
✅ **カラー CLI** - 進捗バーなど高見やすさ  
✅ **80%+テスト** - ユニット&結合含む網羅テスト  
✅ **コード品質** - Ktlint/JaCoCo 対応

---

## 🏗️ アーキテクチャ

```
┌─────────────────┐
│  CSVファイル      │
│ (users.csvなど)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ CSVパーサ       │  ← ヘッダ検証&行取得
│ (kotlin-csv)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Avroスキーマ    │  ← .avsc読込&妥当性
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Avroレコード    │  ← CSV→Avro変換&型変換
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Dry run?        │  ← Kafka送信スキップ=検証のみ
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ バッチ処理       │  ← バッチサイズ/同期非同期
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Producer  │  ← Kafka投入・スキーマ管理
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafkaトピック    │
└─────────────────┘
```

---

## 🛠️ 技術構成

-   **言語:** Kotlin 1.9.22 (JVM 21)
-   **ビルド:** Gradle 8.14 + Kotlin DSL
-   **CLI:** Clikt 4.2.1（コマンド解析）
-   **ターミナル UI:** Mordant 2.2.0（カラーバー等）
-   **CSV:** kotlin-csv-jvm 1.9.2
-   **Avro:** Apache Avro 1.11.3
-   **Kafka:** kafka-clients 3.6.1
-   **Schema Registry:** Confluent 7.5.3
-   **テスト:** JUnit 5・Kotest・Testcontainers・Mockk
-   **品質:** Ktlint 1.0.1・JaCoCo 0.8.11
-   **コンテナ:** Docker/Colima + Testcontainers

---

## 📦 インストール

### 必須要件

-   **Java 21 以上** (JDK)
-   **Docker or Colima** (Kafka ローカル/CI 用途)
-   **Kafka & Schema Registry** (実際の運用では起動必須)

### ソースからビルド

```bash
git clone https://github.com/drag0sd0g/kafka-csv-loader.git
cd kafka-csv-loader

# 全部ビルド（テスト・カバレッジ・lint含む）
./gradlew build

# Fat JAR作成
./gradlew jar

# 実行可能jarは
# build/libs/kafka-csv-loader-*.jar
```

---

### テスト実行

```bash
# 全部テスト
./gradlew test

# カバレッジ付き
./gradlew test jacocoTestReport

# カバレッジレポート表示
open build/reports/jacoco/test/html/index.html

# ユニットテストのみ
./gradlew test --tests "*.csv.*" --tests "*.avro.*"

# 結合テスト（Docker/Colima要）
./gradlew test --tests "*IntegrationTest"
```

---

## 🚀 クイックスタート

### 1. データ準備

`users.csv`例:

```csv
id,name,email,age,active
1,Alice,alice@example.com,30,true
2,Bob,bob@example.com,25,false
3,Charlie,charlie@example.com,35,true
```

`user-schema.avsc`例:

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

---

### 2. Kafka & Schema Registry 起動

```bash
# Docker Compose例
docker-compose up -d kafka schema-registry

# Confluent CLI利用例
confluent local services start
```

---

### 3. Dry Run 検証

投入前の事前バリデーション:

```bash
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --dry-run
```

---

### 4. Kafka へ投入

```bash
# 基本パターン（行単位）
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --bootstrap-servers localhost:9092 \
  --schema-registry http://localhost:8081 \
  --key-field id

# バッチで高速化
java -jar build/libs/kafka-csv-loader-*.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --batch-size 100
```

---

### 5. Kafka 結果確認

```bash
# kafka-avro-console-consumer利用
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users \
  --from-beginning
```

---

## 📖 使い方

### コマンドラインオプション

```
Usage: kafka-csv-loader [OPTIONS]

  Load CSV data into Kafka with Avro schema validation

Options:
  -c, --csv TEXT              CSVファイルパス（必須）
  -s, --schema TEXT           Avroスキーマ(.avsc)（必須）
  -t, --topic TEXT            Kafkaトピック名（必須）
  -b, --bootstrap-servers     Kafka bootstrap（デフォ: localhost:9092）
  -r, --schema-registry       スキーマレジストリURL（デフォ: http://localhost:8081）
  -k, --key-field TEXT        Kafkaキーに使うCSV列名（任意）
  -d, --dry-run               送信せずCSV/スキーマ検証のみ
  --batch-size INT            1バッチの件数(デフォ:1)
  --async                     非同期バッチ送信(より高速)
  --version                   バージョン表示
  -h, --help                  ヘルプ
```

### 利用例

#### 基本(行単位)

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic
```

#### カスタム Kafka 設定

```bash
java -jar kafka-csv-loader.jar \
  --csv data.csv \
  --schema schema.avsc \
  --topic my-topic \
  --bootstrap-servers kafka1:9092,kafka2:9092 \
  --schema-registry http://schema-registry:8081
```

#### 指定カラムを Kafka キー化

```bash
java -jar kafka-csv-loader.jar \
  --csv orders.csv \
  --schema order-schema.avsc \
  --topic orders \
  --key-field order_id
```

#### バッチ投入（大規模向け）

```bash
# 同期バッチ(安全)
java -jar kafka-csv-loader.jar \
  --csv large-file.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100

# 非同期バッチ(最速)
java -jar kafka-csv-loader.jar \
  --csv large-file.csv \
  --schema schema.avsc \
  --topic my-topic \
  --batch-size 100 \
  --async
```

#### Dry Run モード

```bash
java -jar kafka-csv-loader.jar \
  --csv users.csv \
  --schema user-schema.avsc \
  --topic users \
  --dry-run
```

---

## 🔍 Dry Run モード

`--dry-run`で Kafka 未送信の事前データ/スキーマ検証可。

-   ✅ Avro スキーマ読込・検証
-   ✅ CSV 解析
-   ✅ ヘッダ名一致確認
-   ✅ Avro レコード変換可能か型検証
-   ✅ エラー時は行番号付きで詳細表示
-   ❌ **Kafka 接続/送信無し**
-   CI/CD での安全チェックにも

---

## ⚡ バッチ処理とパフォーマンス

ファイル大きいほどバッチ推奨。  
`--batch-size`調整と非同期で劇的高速化。

---

## 🏭 プロジェクト構成

フィルツリーは英語版 README を参照

---

## 🐛 エラーハンドリング

-   スキーマ・型不整合
-   ヘッダ欠損
-   Kafka 接続・タイムアウト
-   バッチ送信の個別失敗

等で丁寧なメッセージ、行番号付きレポートを返します。

---

## 🧪 テスト

-   ユニット：パース、Avro 変換等
-   結合：Testcontainers で Kafka+SR 含む
-   CLI/DryRun 含め 80%超カバレッジ達成

テスト/カバレッジ/lint/自動 CI 全部 Gradle 一発！

---

## 🤝 コントリビュート

PR・issue 歓迎！  
使ったらスターもお願いします ⭐

---

## 📝 ライセンス

MIT License — [LICENSE](LICENSE)

---

## 🙏 謝辞

-   [Clikt](https://ajalt.github.io/clikt/)（CLI 制御）
-   [Mordant](https://github.com/ajalt/mordant)（進捗 UI）
-   [kotlin-csv](https://github.com/doyaaaaaken/kotlin-csv)
-   [Testcontainers](https://www.testcontainers.org/)
-   [Ktlint](https://ktlint.github.io/)
-   [JaCoCo](https://www.jacoco.org/)

---

## 📧 連絡先

**Dragos** - [@drag0sd0g](https://github.com/drag0sd0g)

プロジェクト: [https://github.com/drag0sd0g/kafka-csv-loader](https://github.com/drag0sd0g/kafka-csv-loader)

---

❤️ と ☕ で開発してます
