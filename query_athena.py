
import time
import boto3
import config


def run_query_and_show_results(athena_client, query, description):
    """Run an Athena query, wait for results, and display them."""
    print("─" * 60)
    print(f"{description}")
    print(f"   SQL: {query.strip()}")
    print()

    # Start query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": config.ATHENA_DATABASE},
        ResultConfiguration={
            "OutputLocation": f"s3://{config.BUCKET_NAME}/athena-results/"
        }
    )
    execution_id = response["QueryExecutionId"]

    # Wait for completion
    while True:
        result = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   {state}: {reason}")
            print()
            return
        time.sleep(2)

    # Get statistics
    stats = result["QueryExecution"]["Statistics"]
    data_scanned = stats.get("DataScannedInBytes", 0)
    exec_time = stats.get("EngineExecutionTimeInMillis", 0)

    print(f"   Data scanned: {data_scanned:,} bytes ({data_scanned/1024:.1f} KB)")
    print(f"   Execution time: {exec_time} ms")
    print()

    # Get results
    results = athena_client.get_query_results(
        QueryExecutionId=execution_id
    )

    rows = results["ResultSet"]["Rows"]

    if not rows:
        print("   (no results)")
        print()
        return

    # First row is the header
    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]

    # Calculate column widths for nice formatting
    col_widths = [len(h) for h in headers]
    data_rows = []
    for row in rows[1:]:
        values = [col.get("VarCharValue", "") for col in row["Data"]]
        data_rows.append(values)
        for i, val in enumerate(values):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(val))

    # Print header
    header_line = " | ".join(
        h.ljust(col_widths[i]) for i, h in enumerate(headers)
    )
    print(f"   {header_line}")
    print(f"   {'-' * len(header_line)}")

    # Print data rows
    for values in data_rows:
        row_line = " | ".join(
            v.ljust(col_widths[i]) if i < len(col_widths) else v
            for i, v in enumerate(values)
        )
        print(f"   {row_line}")

    print(f"\n   ({len(data_rows)} rows)")
    print()


def main():
    print("=" * 60)
    print("  Running Athena Queries on Data Lake")
    print("  All queries use partition filters!")
    print("=" * 60)
    print()

    athena = boto3.client("athena", region_name=config.REGION)
    db = config.ATHENA_DATABASE

    # ── Query 1 ──
    run_query_and_show_results(
        athena,
        f"""
        SELECT order_id, user_id, amount, currency, status
        FROM {db}.orders
        WHERE year = 2025 AND month = 1 AND day = 10
        ORDER BY amount DESC
        """,
        "Orders on 2025-01-10 (single day partition)"
    )

    # ── Query 2 ──
    run_query_and_show_results(
        athena,
        f"""
        SELECT status,
               COUNT(*) as order_count,
               ROUND(SUM(amount), 2) as total_revenue
        FROM {db}.orders
        WHERE year = 2025 AND month = 1
        GROUP BY status
        ORDER BY total_revenue DESC
        """,
        "Revenue by status — January 2025 (month partition)"
    )

    # ── Query 3 ──
    run_query_and_show_results(
        athena,
        f"""
        SELECT event_type, COUNT(*) as event_count
        FROM {db}.events
        WHERE year = 2025 AND month = 1 AND day = 10
        GROUP BY event_type
        ORDER BY event_count DESC
        """,
        "Event types on 2025-01-10 (single day partition)"
    )

    # ── Query 4 ──
    run_query_and_show_results(
        athena,
        f"""
        SELECT user_id,
               COUNT(*) as total_orders,
               ROUND(SUM(amount), 2) as total_spent
        FROM {db}.orders
        WHERE year = 2025 AND month = 1
          AND status = 'completed'
        GROUP BY user_id
        ORDER BY total_spent DESC
        """,
        "Top spenders in January 2025 (completed orders only)"
    )

    print("=" * 60)
    print(" All queries completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()