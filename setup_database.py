

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import config


def create_database():
    """Create the database if it doesn't exist."""
    print(f"Connecting to PostgreSQL at {config.DB_HOST}:{config.DB_PORT}...")

    # Connect to default 'postgres' database first
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Check if our database already exists
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (config.DB_NAME,)
    )
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {config.DB_NAME}")
        print(f"  Database '{config.DB_NAME}' created")
    else:
        print(f"  Database '{config.DB_NAME}' already exists")

    cursor.close()
    conn.close()


def create_tables():
    """Create tables and insert sample data."""
    print("Creating tables...")

    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname=config.DB_NAME
    )
    cursor = conn.cursor()

    # ── Create Users table ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            name VARCHAR(255) NOT NULL,
            plan VARCHAR(50) DEFAULT 'free',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("  Users table created")

    # ── Create Orders table ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(3) DEFAULT 'USD',
            status VARCHAR(50) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("  Orders table created")

    # ── Create Events table ──
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            event_type VARCHAR(100) NOT NULL,
            properties JSONB DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("   Events table created")

    # ── Insert sample users ──
    users = [
        ("alice@example.com", "Alice Johnson", "pro"),
        ("bob@example.com", "Bob Smith", "free"),
        ("carol@example.com", "Carol Williams", "enterprise"),
        ("dave@example.com", "Dave Brown", "pro"),
        ("eve@example.com", "Eve Davis", "free"),
    ]

    for email, name, plan in users:
        cursor.execute("""
            INSERT INTO users (email, name, plan)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (email, name, plan))

    print(f"  Inserted {len(users)} users")

    # ── Insert sample orders ──
    orders = [
        (1, 29.99, "USD", "completed", "2025-01-10 10:00:00"),
        (1, 29.99, "USD", "completed", "2025-02-10 10:00:00"),
        (3, 199.99, "USD", "completed", "2025-01-15 14:30:00"),
        (4, 29.99, "USD", "refunded", "2025-01-20 09:15:00"),
        (2, 9.99, "USD", "pending", "2025-03-01 16:45:00"),
    ]

    for user_id, amount, currency, status, created_at in orders:
        cursor.execute("""
            INSERT INTO orders (user_id, amount, currency, status, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, amount, currency, status, created_at))

    print(f"  Inserted {len(orders)} orders")

    # ── Insert sample events ──
    events = [
        (1, "login", '{"device": "mobile"}', "2025-01-10 08:00:00"),
        (1, "purchase", '{"plan": "pro"}', "2025-01-10 10:00:00"),
        (2, "login", '{"device": "desktop"}', "2025-01-11 12:00:00"),
        (3, "login", '{"device": "desktop"}', "2025-01-15 14:00:00"),
        (3, "purchase", '{"plan": "enterprise"}', "2025-01-15 14:30:00"),
    ]

    for user_id, event_type, properties, created_at in events:
        cursor.execute("""
            INSERT INTO events (user_id, event_type, properties, created_at)
            VALUES (%s, %s, %s, %s)
        """, (user_id, event_type, properties, created_at))

    print(f"  Inserted {len(events)} events")

    # ── Show counts ──
    for table in ["users", "orders", "events"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table}: {count} rows")

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("Setting up sample database")
    print("=" * 50)
    print()
    create_database()
    create_tables()
    print()
    print(" Database setup complete!")