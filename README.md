# Data Engineering Assessment

This repository contains a collection of Python scripts and utilities used in a
simple data engineering assessment. The goal is to demonstrate a minimal data
pipeline that covers:

- PostgreSQL database setup and sample data ingestion
- Generating synthetic data files and uploading to an S3 data lake
- Configuring Amazon Athena tables over the data lake
- Running sample queries against Athena
- Backing up the PostgreSQL database to S3 with retention policy
- Cleaning up AWS resources when finished

All configuration values (database credentials, bucket name, AWS region, etc.)
are stored in `config.py` so you can adjust them for your own environment.

---

## Prerequisites

Before running the scripts you'll need:

1. **Python 3.10+** installed on your machine.
2. [pip](https://pip.pypa.io/en/stable/) for installing dependencies.
3. AWS credentials configured (e.g. via `aws configure` or environment
   variables) with permissions for S3, Athena, and STS.
4. PostgreSQL server running locally (default host `localhost:5432`).
   The `pg_dump` command‑line utility must be available on your `PATH`.
5. The following Python packages installed:
   ```bash
   pip install boto3 pandas pyarrow psycopg2-binary
   ```
   (You can also install them via a virtual environment.)

---

## Configuration

Edit `config.py` to match your environment. Key settings include:

- `BUCKET_NAME` – S3 bucket used for backups and data lake.
- `REGION` – AWS region for S3/Athena.
- PostgreSQL connection info (`DB_HOST`, `DB_PORT`, `DB_USER`, etc.).
- Prefixes for S3 paths (`BACKUP_PREFIX`, `DATALAKE_PREFIX`).
- Athena database name and retention days for backups.

---

## Scripts and Usage

Each Python file is a self‑contained utility. Run them directly from the
command line (e.g. `python setup_database.py`).

### `setup_database.py`
Create the PostgreSQL database, define the `users`, `orders`, and `events`
schemas, and insert some sample rows.

### `connection.py`
Quick sanity check to verify a PostgreSQL connection can be established.

### `create_bucket.py`
Create the S3 bucket specified in `config.py`. Handles existing buckets and
reports errors if the name is already taken by another account.

### `generate_data.py`
Generate synthetic `orders` and `events` data for a few hard‑coded dates and
write them as partitioned Parquet files under `output/`.  This simulates a data
lake ingestion pipeline.

### `upload_datalake.py`
Recursively upload the contents of the `output/` directory to the S3 data lake
prefix.  Also lists the files in the bucket after upload for verification.

### `setup_athena.py`
Using the `boto3` Athena client, create an Athena database and two external
tables (`orders` and `events`) that point to the Parquet files in S3.  The
tables are partitioned by year/month/day and the script repairs partitions
after creation.

### `query_athena.py`
Run a handful of example queries against the Athena tables to demonstrate
partition filters, aggregations, and scans.  Results and stats are printed to
stdout.

### `backup_to_s3.py`
Perform a full PostgreSQL dump using `pg_dump`, compress it with `gzip`, and
upload it to the S3 backup prefix.  The script includes prerequisite checks,
upload verification, and an S3 object tagging based retention policy that
later can be used by lifecycle rules to expire old backups.

### `cleanup.py`
Remove all AWS resources created by the assessment: drop Athena tables and
database, delete all objects from the bucket (and the bucket itself), and
delete any locally generated directories like `output/` and temporary backup
files.  Use with caution; the script prompts for confirmation.

---

## Typical Workflow

1. Edit `config.py` with your settings.
2. Ensure PostgreSQL is running and `pg_dump` is on the path.
3. Install dependencies (see above).
4. Run `python setup_database.py` to prepare the sample database.
5. (Optional) Use `python connection.py` to test the DB connection.
6. Run `python create_bucket.py` to make the S3 bucket.
7. Generate sample data: `python generate_data.py`.
8. Upload the data lake: `python upload_datalake.py`.
9. Configure Athena: `python setup_athena.py`.
10. Query the data: `python query_athena.py`.
11. Take a backup: `python backup_to_s3.py`.
12. When finished, you can clean up everything with
    `python cleanup.py`.

---

## Notes & Tips

- All scripts are idempotent where possible; you can safely re‑run them.
- Adjust the list of dates and users in `generate_data.py` to simulate other
datasets.
- The backup script tags S3 objects so you can configure lifecycle policies in
  the AWS console to automatically expire old backups.
- If you need to change Python dependencies, consider adding a
  `requirements.txt` file and updating documentation accordingly.

Happy data engineering! 🎯
