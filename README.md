# Data Engineering Assessment — Backup & Data Lake Solution

<br>

## Author

| Field | Value |
|-------|-------|
| Name | Rishabh Pal |
| Date | 27-02-2026 |
| Email | rishabh.pal.rcc390@gmail.com|

<br>

---

<br>

## Project Objective

This assessment is designed to evaluate your practical data engineering skills, specifically:
Cost-aware decision making
Backup automation
Data lake design for analytics
Ability to execute safely on AWS without incurring cost
<br>

### What This Project Does

<br>

**Part 1 — Cost Analysis**
- Compared 3 different backup strategies
- Recommended the best approach for production
- Analyzed cost, restore speed, and operational complexity

<br>

**Part 2 — Backup Automation**
- Takes PostgreSQL backup using pg_dump
- Compresses the backup with gzip
- Uploads to S3 organized by date
- Deletes old backups automatically (retention policy)

<br>

**Part 3 — Data Lake**
- Generates sample data in Parquet format
- Uploads to S3 with date-based partitions
- Creates Athena tables with partition columns
- Runs SQL queries scanning only KBs of data

<br>

**Cleanup**
- Deletes ALL AWS resources after execution
- Total AWS cost: $0.00

<br>

---

<br>

## Project Structure

<br>

| File | Purpose |
|------|---------|
| `README.md` | This documentation file |
| `COST_ANALYSIS.md` | Part 1: Cost analysis document |
| `config.py` | Shared settings (bucket name, DB credentials) |
| `create_bucket.py` | Creates S3 bucket |
| `setup_database.py` | Creates sample PostgreSQL database |
| `setup_athena_config.py` | Configures Athena query result location |
| `backup_to_s3.py` | Part 2: Backup automation script |
| `generate_data.py` | Part 3: Generates sample Parquet files |
| `upload_datalake.py` | Part 3: Uploads data to S3 |
| `setup_athena.py` | Part 3: Creates Athena database and tables |
| `query_athena.py` | Part 3: Runs Athena queries with partition filters |
| `cleanup.py` | Deletes ALL AWS resources |

<br>

### Screenshots Folder

<br>

| Screenshot | What It Shows |
|------------|---------------|
| `backup_execution.png` | Backup script running successfully |
| `s3_datalake_upload.png` | S3 file structure after upload |
| `athena_query_results.png` | Query results with data scanned proof |
| `athena_console.png` | Athena console in AWS |
| `cleanup_output.png` | Cleanup script deleting everything |
| `billing_zero.png` | AWS billing page showing $0.00 |

<br>

---

<br>

## Prerequisites

<br>

### Software Required

<br>

| Software | Version | Purpose |
|----------|---------|---------|
| Python | 3.8 or higher | All scripts are written in Python |
| PostgreSQL | 14 or higher | Local database for backup demo |
| AWS CLI | 2.x | AWS credential configuration |

<br>

### Python Libraries Required

<br>

| Library | Purpose |
|---------|---------|
| `boto3` | Communicate with AWS services (S3, Athena) |
| `pandas` | Create and manipulate data tables |
| `pyarrow` | Read and write Parquet files |
| `psycopg2-binary` | Connect Python to PostgreSQL |

<br>

**Install all libraries with one command:**

```bash
pip3 install boto3 pandas pyarrow psycopg2-binary
```

<br>

### AWS Permissions Required

<br>

| IAM Policy | Why |
|------------|-----|
| `AmazonS3FullAccess` | Create bucket, upload files, delete files |
| `AmazonAthenaFullAccess` | Create tables, run queries |

<br>

---

<br>

## How To Run — Step by Step

<br>

### Step 0: Configure Settings

<br>

Open `config.py` and change these two values:

```python
BUCKET_NAME = "de-assessment-yourname-2025"   # Your unique bucket name
DB_PASSWORD = "postgres"                       # Your PostgreSQL password
```

<br>

### Step 1: Verify Everything Is Installed

<br>

```bash
python3 --version
```

```bash
python3 -c "import boto3, pandas, pyarrow, psycopg2; print('All libraries OK')"
```

```bash
aws sts get-caller-identity
```

```bash
psql --version
```

<br>

### Step 2: Create S3 Bucket

<br>

```bash
python3 create_bucket.py
```

<br>

Expected output:

```
Creating S3 bucket: de-assessment-yourname-2025
  ✓ Bucket created successfully!
  ✓ Verified: bucket exists in your account
```

<br>

### Step 3: Create Sample Database

<br>

```bash
python3 setup_database.py
```

<br>

Expected output:

```
  ✓ Database 'saas_platform' created
  ✓ Users table created
  ✓ Orders table created
  ✓ Events table created
  📊 users: 5 rows
  📊 orders: 5 rows
  📊 events: 5 rows
```

<br>

### Step 4: Run Backup (Part 2)

<br>

```bash
python3 backup_to_s3.py
```

<br>

What this script does:

1. Runs `pg_dump` to export the entire database as SQL
2. Compresses the SQL file using gzip (reduces size by ~80%)
3. Uploads the compressed file to S3
4. Organizes by date: `s3://bucket/backups/postgres/2025/06/10/backup.sql.gz`
5. Checks for backups older than 30 days and deletes them
6. Removes the local temporary file

<br>

### Step 5: Generate Data Lake Files (Part 3)

<br>

```bash
python3 generate_data.py
```

<br>

What this script does:

- Creates Parquet files for `orders` and `events` tables
- Generates data for 6 different dates
- Saves files in Hive-style partition folders

<br>

Output folder structure:

```
output/
├── orders/
│   └── year=2025/
│       ├── month=01/
│       │   ├── day=10/data.parquet
│       │   ├── day=11/data.parquet
│       │   └── day=12/data.parquet
│       ├── month=02/
│       │   ├── day=01/data.parquet
│       │   └── day=02/data.parquet
│       └── month=03/
│           └── day=01/data.parquet
└── events/
    └── (same structure as orders)
```

<br>

### Step 6: Upload Data to S3

<br>

```bash
python3 upload_datalake.py
```

<br>

### Step 7: Configure Athena

<br>

```bash
python3 setup_athena_config.py
```

<br>

This sets the query result location to `s3://bucket/athena-results/` and runs a test query to verify Athena works.

<br>

### Step 8: Create Athena Tables

<br>

```bash
python3 setup_athena.py
```

<br>

What this script does:

1. Creates database `saas_datalake`
2. Creates `orders` table with partition columns (year, month, day)
3. Creates `events` table with partition columns (year, month, day)
4. Runs `MSCK REPAIR TABLE` to discover all partitions in S3

<br>

### Step 9: Run Athena Queries

<br>

```bash
python3 query_athena.py
```

<br>

What this script does:

- Runs 5 SQL queries
- Every query includes partition filters in the WHERE clause
- Shows data scanned for each query (must be KBs only)
- Displays formatted results

<br>

### Step 10: Cleanup (MANDATORY)

<br>

```bash
python3 cleanup.py
```

<br>

What this script does:

1. Drops Athena tables (orders, events)
2. Drops Athena database (saas_datalake)
3. Deletes ALL objects in the S3 bucket
4. Deletes the S3 bucket itself
5. Removes local generated files

<br>

---

<br>

## Part 2 — Backup Details

<br>

### Backup Flow

<br>

| Step | Action | Tool Used |
|------|--------|-----------|
| 1 | Export database to SQL file | `pg_dump` |
| 2 | Compress SQL file | `gzip` (Python) |
| 3 | Upload to S3 | `boto3` |
| 4 | Delete old backups | `boto3` |
| 5 | Remove local temp file | `os.remove` |

<br>

### S3 Backup Path Structure

<br>

```
s3://bucket/backups/postgres/
└── 2025/
    └── 06/
        └── 10/
            └── backup_20250610_143022.sql.gz
```

<br>

### How Retention Works

<br>

**Layer 1 — Script-based retention (active):**

- Every time `backup_to_s3.py` runs, it scans S3 for existing backups
- Any backup with a `LastModified` date older than 30 days is deleted
- This runs automatically as part of every backup

<br>

**Layer 2 — S3 Lifecycle Policy (safety net):**

- Can be configured on the S3 bucket as an extra safety measure
- Automatically moves old backups to cheaper storage
- Automatically deletes backups after the retention period
- Works even if the backup script fails to run

<br>

---

<br>

## Part 3 — Data Lake Details

<br>

### Why Parquet Format Instead of CSV?

<br>

| Feature | CSV | Parquet |
|---------|-----|---------|
| File type | Text (row-based) | Binary (column-based) |
| File size | Large | Small (compressed) |
| What Athena reads | ALL columns always | Only the columns you need |
| Query cost | Higher | Lower |

<br>

When you run `SELECT amount, status FROM orders`, Athena with Parquet only reads the `amount` and `status` columns. With CSV, it would read every column in the file.

<br>

### Why Partitions?

<br>

**Without partitions:**

- All data in one big file
- Every query must scan everything
- More data scanned = more cost

<br>

**With partitions:**

- Data split into folders by date
- Queries with `WHERE year=2025 AND month=1` only scan matching folders
- Much less data scanned = much less cost

<br>

### Athena Query Rules (Assessment Requirement)

<br>

**Rule 1:** Every query MUST have partition filters in the WHERE clause.

<br>

**Rule 2:** Data scanned must be in KBs or MBs only, NOT GBs.

<br>

**Correct — has partition filter:**

```sql
SELECT * FROM orders
WHERE year = 2025 AND month = 1 AND day = 10;
```

<br>

**Wrong — no partition filter:**

```sql
SELECT * FROM orders;
```

<br>

### Queries Used in This Project

<br>

| Query | Description | Partition Filter | Data Scanned |
|-------|-------------|-----------------|--------------|
| 1 | Orders on 2025-01-10 | year=2025, month=1, day=10 | ~2 KB |
| 2 | Revenue by status, Jan 2025 | year=2025, month=1 | ~6 KB |
| 3 | Event types on 2025-01-10 | year=2025, month=1, day=10 | ~3 KB |
| 4 | Top spenders, Jan 2025 | year=2025, month=1 | ~6 KB |
| 5 | Login counts, Jan 2025 | year=2025, month=1 | ~3 KB |

<br>

---

<br>

## Cost Summary

<br>

### AWS Resources Used

<br>

| Resource | What For | Amount Used | Cost |
|----------|----------|-------------|------|
| S3 Storage | Backups and data lake files | Less than 1 MB | $0.00 |
| S3 PUT Requests | Uploading files | Less than 20 | $0.00 |
| S3 GET Requests | Athena reading files | Less than 50 | $0.00 |
| Athena Queries | Running SQL queries | Less than 100 KB scanned | $0.00 |
| **Total** | | | **$0.00** |

<br>

### Why It Costs Nothing

<br>

- S3 charges $0.023 per GB per month. We used less than 0.001 GB.
- Athena charges $5 per TB scanned. We scanned less than 0.0001 GB.
- Both round to $0.00.

<br>

### Prohibited Resources — NOT Used

<br>

| Resource | Status |
|----------|--------|
| RDS | ❌ Not used |
| EC2 | ❌ Not used |
| DMS | ❌ Not used |
| Glue Jobs | ❌ Not used |
| NAT Gateway | ❌ Not used |
| QuickSight | ❌ Not used |
| Any managed database | ❌ Not used |
| Any compute service | ❌ Not used |

<br>

---

<br>

## Cleanup Proof

<br>

### What Was Deleted

<br>

| Resource | Action |
|----------|--------|
| Athena table: `orders` | Dropped |
| Athena table: `events` | Dropped |
| Athena database: `saas_datalake` | Dropped |
| All S3 objects | Deleted |
| S3 bucket | Deleted |
| Local `output/` folder | Deleted |

<br>

### Verification

<br>

- S3 bucket confirmed deleted (returns 404)
- Athena database and tables no longer exist
- Local files removed
- AWS Billing page shows $0.00

<br>

See screenshot: `screenshots/billing_zero.png`

<br>

---

<br>

## Quick Reference — All Commands in Order

<br>

```bash
python3 create_bucket.py
python3 setup_database.py
python3 backup_to_s3.py
python3 generate_data.py
python3 upload_datalake.py
python3 setup_athena_config.py
python3 setup_athena.py
python3 query_athena.py
python3 cleanup.py
```

<br>

---

<br>



---

<br>

## Technologies Used

<br>

| Technology | Purpose |
|-----------|---------|
| Python 3 | All scripts and automation |
| PostgreSQL | Sample production database |
| AWS S3 | Object storage for backups and data lake |
| AWS Athena | Serverless SQL query engine |
| Apache Parquet | Columnar file format for analytics |
| Hive-style Partitioning | Date-based folder organization for efficient queries |
| boto3 | AWS SDK for Python |
| pandas | Data generation and manipulation |
| pg_dump | PostgreSQL backup utility |
| gzip | File compression |
testing
