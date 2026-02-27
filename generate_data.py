
import os
import random
import pandas as pd 
from datetime import datetime

random.seed(42)

OUTPUT_DIR = "output"

DATES = [
    datetime(2025, 1, 10),
    datetime(2025, 1, 11),
    datetime(2025, 1, 12),
    datetime(2025, 2, 1),
    datetime(2025, 2, 2),
    datetime(2025, 3, 1),
]

USERS = [
    {"id": 1, "email": "alice@example.com"},
    {"id": 2, "email": "bob@example.com"},
    {"id": 3, "email": "carol@example.com"},
    {"id": 4, "email": "dave@example.com"},
    {"id": 5, "email": "eve@example.com"},
]


def generate_orders(date, num_orders):
    """Generate random order records for one day."""
    records = []
    for i in range(num_orders):
        records.append({
            "order_id": f"ord_{date.strftime('%Y%m%d')}_{i+1:04d}",
            "user_id": random.choice(USERS)["id"],
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "status": random.choice(["completed", "pending", "refunded"]),
            "created_at": date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            ).isoformat(),
        })
    return pd.DataFrame(records)


def generate_events(date, num_events):
    """Generate random event records for one day."""
    records = []
    for i in range(num_events):
        user = random.choice(USERS)
        event_type = random.choice(["login", "logout", "page_view", "purchase", "signup"])

        records.append({
            "event_id": f"evt_{date.strftime('%Y%m%d')}_{i+1:04d}",
            "user_id": user["id"],
            "user_email": user["email"],
            "event_type": event_type,
            "properties": str({"device": random.choice(["mobile", "desktop", "tablet"])}),
            "created_at": date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            ).isoformat(),
        })
    return pd.DataFrame(records)


def save_parquet(df, table_name, date):
    """Save DataFrame as a Parquet file in a partitioned directory."""
    partition_path = os.path.join(
        OUTPUT_DIR,
        table_name,
        f"year={date.year}",
        f"month={date.month:02d}",
        f"day={date.day:02d}"
    )
    os.makedirs(partition_path, exist_ok=True)

    filepath = os.path.join(partition_path, "data.parquet")
    df.to_parquet(filepath, engine="pyarrow", index=False)

    size = os.path.getsize(filepath)
    print(f"{filepath}")
    print(f"{len(df)} rows, {size:,} bytes")


def main():
    print("=" * 55)
    print("  Generating Data Lake Files")
    print("=" * 55)
    print()

    total_orders = 0
    total_events = 0

    for date in DATES:
        date_str = date.strftime("%Y-%m-%d")
        print(f"{date_str}")

        # Generate orders
        num_orders = random.randint(5, 15)
        orders_df = generate_orders(date, num_orders)
        save_parquet(orders_df, "orders", date)
        total_orders += num_orders

        # Generate events
        num_events = random.randint(10, 30)
        events_df = generate_events(date, num_events)
        save_parquet(events_df, "events", date)
        total_events += num_events

        print()

    print("=" * 55)
    print(f"Generated {total_orders} orders + {total_events} events")
    print(f"Files saved in: {OUTPUT_DIR}/")
    print("=" * 55)


if __name__ == "__main__":
    main()