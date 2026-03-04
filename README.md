# 🏨 Hotel ETL Pipeline

A production-style ETL (Extract, Transform, Load) pipeline built with Python, PostgreSQL, and AWS S3 designed to process real-world hotel booking data with data cleaning, validation, optimization, and cloud integration.

 Screen Recording : https://youtu.be/2-6jh-qjmsw?si=uL1SZkVaVsflZG6T

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [ETL Pipeline](#etl-pipeline)
- [Database Design & Optimization](#database-design--optimization)
- [AWS S3 Integration](#aws-s3-integration)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Analytical Queries](#analytical-queries)
- [Scalability & Architecture Thinking](#scalability--architecture-thinking)

---

## 📌 Project Overview

This project simulates a real-world data engineering workflow:

- Generates a **12,000+ record** dirty hotel dataset
- Runs a **fully automated ETL pipeline** with one command
- Cleans, validates, and standardizes raw data
- Loads clean data into **PostgreSQL** with proper schema design
- Backs up raw and cleaned files to **AWS S3**
- Logs all rejected records separately for auditing

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Data Processing | Pandas, NumPy |
| Database | PostgreSQL 15 |
| ORM / Connector | SQLAlchemy, psycopg2 |
| Cloud Storage | AWS S3 (boto3) |
| Logging | Loguru |
| Fake Data | Faker |
| Config | python-dotenv |

---

## 📁 Project Structure

```
hotel_etl/
├── data/
│   ├── raw/                  # Raw generated CSV
│   └── cleaned/              # Cleaned CSV + rejected records
├── etl/
│   ├── __init__.py
│   ├── extract.py            # Extract from local + upload to S3
│   ├── transform.py          # Clean, validate, standardize
│   ├── load.py               # Load into PostgreSQL
│   └── logger.py             # Loguru logging setup
├── sql/
│   ├── schema.sql            # Table schema + indexes
│   └── queries.sql           # 3 analytical queries
├── logs/
│   ├── pipeline.log          # Full pipeline log
│   └── rejected_records.log  # Rejected record log
├── generate_dataset.py       # Dataset generator (auto-called)
├── run_pipeline.py           # Main pipeline entry point
├── requirements.txt
├── .env.example
└── README.md
```

---

## 📊 Dataset

The dataset is **auto-generated** when you run the pipeline. It simulates real-world dirty hotel data with **12,300 records** and intentional data quality issues.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | Integer | Unique hotel ID |
| `name` | String | Hotel name |
| `category` | String | Hotel type (Luxury, Budget, etc.) |
| `price` | Float | Price per night (USD) |
| `rating` | Float | Guest rating (1.0 – 5.0) |
| `country` | String | Country of hotel |
| `created_date` | Date | Listing creation date |
| `rooms_available` | Integer | Number of available rooms |
| `reviews_count` | Integer | Total number of reviews |

### Intentional Data Issues

| Issue Type | Example |
|------------|---------|
| Missing values | `name = NULL`, `price = NULL` |
| Duplicate rows | 300 duplicate records injected |
| Inconsistent casing | `'usa'`, `'USA'`, `'india'`, `'FRANCE'` |
| Invalid values | `rating = 99`, `rating = -1` |
| Mixed date formats | `2023-06-15` vs `15/06/2023` |
| Empty strings | `category = ''` |

---

## ⚙️ ETL Pipeline

The pipeline runs in 3 stages:

### 1. Extract
- Reads raw CSV from `data/raw/hotels_raw.csv`
- Auto-generates the dataset if the file doesn't exist
- Uploads raw file to AWS S3 (`raw/hotels_raw.csv`)

### 2. Transform
- **Remove duplicates** — drops duplicate rows by `id`
- **Standardize casing** — `title()` for category, `upper()` for country
- **Parse mixed date formats** — handles `YYYY-MM-DD` and `DD/MM/YYYY`
- **Validate constraints** — rejects records with invalid rating, missing name, negative price, or unknown category
- **Fill nulls** — median imputation for `rooms_available`, 0 for `reviews_count`
- **Log rejected records** — saved to `data/cleaned/rejected_records.csv` and `logs/rejected_records.log`

### 3. Load
- Loads cleaned data into PostgreSQL `hotels` table
- Uploads cleaned CSV to AWS S3 (`cleaned/hotels_clean.csv`)
- Saves rejected records to CSV

---

## 🗄️ Database Design & Optimization

### Schema

```sql
CREATE TABLE IF NOT EXISTS hotels (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    price           NUMERIC(10,2) CHECK (price > 0),
    rating          NUMERIC(3,1) CHECK (rating BETWEEN 1 AND 5),
    country         VARCHAR(100),
    created_date    DATE,
    rooms_available INTEGER,
    reviews_count   INTEGER DEFAULT 0
);
```

### Indexes

```sql
CREATE INDEX idx_hotels_category ON hotels(category);
CREATE INDEX idx_hotels_country  ON hotels(country);
CREATE INDEX idx_hotels_rating   ON hotels(rating);
CREATE INDEX idx_hotels_created  ON hotels(created_date);
```

### Why These Indexes?

| Index | Reason |
|-------|--------|
| `idx_hotels_category` | Most analytical queries filter or group by category |
| `idx_hotels_country` | Country-level aggregations are frequent |
| `idx_hotels_rating` | Range queries (`rating >= 4.0`) benefit from B-tree index |
| `idx_hotels_created` | Date range queries for monthly growth analysis |

### Index Performance Demo

```sql
-- Disable indexes 
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;
EXPLAIN ANALYZE SELECT * FROM hotels WHERE category = 'Luxury' AND rating >= 4.0;

-- Re-enable indexes
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;
EXPLAIN ANALYZE SELECT * FROM hotels WHERE category = 'Luxury' AND rating >= 4.0;
```

---

## ☁️ AWS S3 Integration

The pipeline integrates with AWS S3 at two stages:

| Stage | S3 Key | Purpose |
|-------|--------|---------|
| After Extract | `raw/hotels_raw.csv` | Archive raw source data |
| After Transform | `cleaned/hotels_clean.csv` | Backup cleaned output |

### IAM Policy (Least Privilege)

Only the minimum required permissions are granted:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::hotel-etl-bucket",
        "arn:aws:s3:::hotel-etl-bucket/*"
      ]
    }
  ]
}
```

All credentials are stored in `.env` — **no hardcoded secrets anywhere in the codebase.**

---

## 🚀 Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Chamindu77/Hotel-Booking-ETL-Pipeline.git
cd hotel-etl-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hotel_db
DB_USER=postgres
DB_PASSWORD=yourpassword

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
```

### 4. Create PostgreSQL Database & Schema

```bash
psql -U postgres -c "CREATE DATABASE hotel_db;"
psql -U postgres -d hotel_db -f sql/schema.sql
```

---

## ▶️ Running the Pipeline

```bash
python run_pipeline.py
```

That's it — **one command** runs the entire pipeline:

```
=== ETL Pipeline Started ===
Raw dataset not found. Generating dataset...
Generated 12300 records
Extracted 12300 raw records
Uploaded raw/hotels_raw.csv to S3
Removed 300 duplicates
Clean records: 10842 | Rejected: 1158
Loaded 10842 records into PostgreSQL
Uploaded cleaned/hotels_clean.csv to S3
Saved 1131 rejected records to CSV
=== ETL Pipeline Completed ===
```

---

## 📈 Analytical Queries

Run these in PostgreSQL to validate data and gain insights:

### 1. Top Categories by Revenue

```sql
SELECT
    category,
    COUNT(*)                         AS total_hotels,
    ROUND(SUM(price)::NUMERIC, 2)    AS total_revenue,
    ROUND(AVG(price)::NUMERIC, 2)    AS avg_price,
    ROUND(MIN(price)::NUMERIC, 2)    AS min_price,
    ROUND(MAX(price)::NUMERIC, 2)    AS max_price
FROM hotels
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;
```

### 2. Monthly Growth Analysis

```sql
SELECT
    TO_CHAR(DATE_TRUNC('month', created_date), 'YYYY-MM')  AS month,
    COUNT(*)                                               AS new_listings,
    ROUND(AVG(price)::NUMERIC, 2)                          AS avg_price,
    ROUND(AVG(rating)::NUMERIC, 2)                         AS avg_rating,
    SUM(COUNT(*)) OVER (
        ORDER BY DATE_TRUNC('month', created_date)
    )                                                      AS cumulative_listings
FROM hotels
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month;
```

### 3. Average Rating by Country

```sql
SELECT
    country,
    COUNT(*)                         AS hotel_count,
    ROUND(AVG(rating)::NUMERIC, 2)   AS avg_rating,
    ROUND(AVG(price)::NUMERIC, 2)    AS avg_price,
    SUM(reviews_count)               AS total_reviews,
    ROUND(AVG(rooms_available))      AS avg_rooms
FROM hotels
GROUP BY country
ORDER BY avg_rating DESC, hotel_count DESC;
```

---

## 📐 Scalability & Architecture Thinking

### 🔼 Scaling to 1 Million+ Records

The current pipeline processes data in-memory using Pandas, which works well up to ~500K records on a standard machine. For 1M+ records, the following changes would be applied:

**Chunked Processing** — instead of loading the full file at once, the pipeline would read and process data in chunks:

```python
for chunk in pd.read_csv('data/raw/hotels_raw.csv', chunksize=50000):
    clean_chunk, rejected_chunk = clean_data(chunk)
    load_to_postgres(clean_chunk)
```

**Distributed Processing** — for very large datasets (10M+ records), the pipeline would migrate to Apache Spark or AWS Glue, which distribute processing across multiple workers and handle data that doesn't fit in memory.

**Database Bulk Loading** — instead of row-by-row inserts, use PostgreSQL `COPY` command or SQLAlchemy bulk operations for significantly faster writes.

---

### ⏰ Scheduling (Cron / Airflow)

**Simple Scheduling with Cron** — for daily or hourly pipeline runs, a cron job can be set up on any Linux server:

```bash
# Run pipeline every day at 2:00 AM
0 2 * * * /usr/bin/python3 /path/to/hotel_etl/run_pipeline.py >> /var/log/etl_cron.log 2>&1
```

**Production Scheduling with Apache Airflow** — for more complex workflows with dependencies, retries, and monitoring, Apache Airflow would be used. Each ETL stage becomes a separate task in a DAG (Directed Acyclic Graph):

```
[Generate/Extract] → [Transform] → [Load to PostgreSQL] → [Upload to S3]
```

Airflow provides a web UI to monitor runs, view logs, trigger manual runs, and set up alerts for failures — making it the industry standard for production pipeline scheduling.

---

### 🗂️ Partitioning & Indexing Strategy at Scale

**Current setup** works well for tens of thousands of records. As data grows, the strategy would evolve:

**Table Partitioning by Date** — splitting the `hotels` table into monthly partitions dramatically speeds up date-range queries, as PostgreSQL only scans the relevant partition instead of the full table:

```sql
-- Partition by created_date (monthly)
CREATE TABLE hotels_2024_01 PARTITION OF hotels
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE hotels_2024_02 PARTITION OF hotels
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
```

**Composite Indexes** — as query patterns become more complex, multi-column indexes on frequently combined filters would be added:

```sql
-- For queries filtering by both category and rating together
CREATE INDEX idx_hotels_category_rating ON hotels(category, rating);

-- For queries filtering by country and date range
CREATE INDEX idx_hotels_country_date ON hotels(country, created_date);
```

**Index Maintenance** — at scale, indexes are periodically rebuilt to prevent bloat:

```sql
REINDEX INDEX CONCURRENTLY idx_hotels_category;
```

---

### 🛡️ Failure Handling

**Stage-Level Try/Catch** — each ETL stage (extract, transform, load) is wrapped in error handling so a failure in one stage does not silently corrupt data in another.

**Rejected Record Logging** — records that fail validation are never silently dropped. They are written to `data/cleaned/rejected_records.csv` and `logs/rejected_records.log` with a clear rejection reason for every record, enabling data teams to audit and reprocess them.

**Idempotent Loading** — the pipeline uses `if_exists='append'` with deduplication logic, ensuring that re-running the pipeline after a failure does not create duplicate records.

**Dead Letter Queue (Production)** — in a cloud-native setup, failed S3 events or processing errors would be routed to an AWS SQS Dead Letter Queue, allowing failed messages to be replayed without data loss.

**Alerting** — in a production Airflow deployment, email or Slack alerts would be configured to notify the data team immediately when a DAG run fails, with the error and failed task clearly identified.

---


