
import time
import boto3
import config


def run_athena_query(athena_client, query, description):
    """Run a query in Athena and wait for it to finish."""
    print(f"  Running: {description}...")

    # Start the query
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": f"s3://{config.BUCKET_NAME}/athena-results/"
        }
    )

    execution_id = response["QueryExecutionId"]
    print(f"    Execution ID: {execution_id}")

    # Wait for query to complete
    while True:
        result = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            print(f"    ✓ SUCCEEDED")
            return execution_id

        elif state == "FAILED":
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"    ✗ FAILED: {reason}")
            return None

        elif state == "CANCELLED":
            print(f"    ✗ CANCELLED")
            return None

        # Still running, wait a bit
        time.sleep(2)


def main():
    print("=" * 55)
    print("  Setting Up Athena Tables")
    print("=" * 55)
    print()

    athena = boto3.client("athena", region_name=config.REGION)
    db = config.ATHENA_DATABASE
    bucket = config.BUCKET_NAME
    prefix = config.DATALAKE_PREFIX

    # ── Step 1: Create database ──
    print("[1/5] Create database")
    run_athena_query(
        athena,
        f"CREATE DATABASE IF NOT EXISTS {db};",
        f"Creating database '{db}'"
    )
    print()

    # ── Step 2: Create orders table ──
    print("[2/5] Create orders table")
    run_athena_query(
        athena,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.orders (
            order_id STRING,
            user_id INT,
            amount DOUBLE,
            currency STRING,
            status STRING,
            created_at STRING
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS PARQUET
        LOCATION 's3://{bucket}/{prefix}/orders/'
        """,
        "Creating orders table"
    )
    print()

    # ── Step 3: Create events table ──
    print("[3/5] Create events table")
    run_athena_query(
        athena,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.events (
            event_id STRING,
            user_id INT,
            user_email STRING,
            event_type STRING,
            properties STRING,
            created_at STRING
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS PARQUET
        LOCATION 's3://{bucket}/{prefix}/events/'
        """,
        "Creating events table"
    )
    print()

    # ── Step 4: Load order partitions ──
    print("[4/5] Load order partitions")
    run_athena_query(
        athena,
        f"MSCK REPAIR TABLE {db}.orders;",
        "Scanning S3 for order partitions"
    )
    print()

    # ── Step 5: Load event partitions ──
    print("[5/5] Load event partitions")
    run_athena_query(
        athena,
        f"MSCK REPAIR TABLE {db}.events;",
        "Scanning S3 for event partitions"
    )
    print()

    print("=" * 55)
    print("  Athena setup complete!")
    print(f"  Database: {db}")
    print(f"  Tables: orders, events")
    print("=" * 55)


if __name__ == "__main__":
    main()