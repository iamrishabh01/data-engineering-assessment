
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="12345678", 
        dbname="postgres"
    )
    print("PostgreSQL connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
    print("\nTroubleshooting:")
    print("1. Is PostgreSQL running?")
    print("2. Is the password correct?")
    print("3. Is it listening on port 5432?")