
import os
import subprocess
import gzip
import shutil
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError
import config


def check_prerequisites():
    """Verify everything is ready before starting."""
    print("[1/5] Checking prerequisites...")

    # Check pg_dump is installed
    try:
        result = subprocess.run(
            ["pg_dump", "--version"],
            capture_output=True, text=True
        )
        print(f"  ✓ pg_dump found: {result.stdout.strip()}")
    except FileNotFoundError:
        print("  ✗ pg_dump not found! Install PostgreSQL client tools.")
        return False

    # Check AWS credentials
    try:
        sts = boto3.client("sts", region_name=config.REGION)
        identity = sts.get_caller_identity()
        print(f"  ✓ AWS credentials valid (Account: {identity['Account']})")
    except Exception as e:
        print(f"  ✗ AWS credentials invalid: {e}")
        return False

    # Check S3 bucket exists
    try:
        s3 = boto3.client("s3", region_name=config.REGION)
        s3.head_bucket(Bucket=config.BUCKET_NAME)
        print(f"  ✓ S3 bucket exists: {config.BUCKET_NAME}")
    except ClientError:
        print(f"  ✗ S3 bucket '{config.BUCKET_NAME}' not found!")
        print("    → Run create_bucket.py first")
        return False

    return True


def take_backup():
    """Run pg_dump and save to a local file."""
    print("\n[2/5] Taking database backup...")

    # Create temp directory
    backup_dir = os.path.join(os.path.expanduser("~"), "pg_backups_temp")
    os.makedirs(backup_dir, exist_ok=True)

    # File names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_file = os.path.join(backup_dir, f"backup_{timestamp}.sql")
    gz_file = sql_file + ".gz"

    # Run pg_dump
    # This creates a text file with all SQL commands to recreate the database
    print(f"  Running pg_dump on database '{config.DB_NAME}'...")

    env = os.environ.copy()
    env["PGPASSWORD"] = config.DB_PASSWORD

    result = subprocess.run(
        [
            "pg_dump",
            "-h", config.DB_HOST,
            "-p", str(config.DB_PORT),
            "-U", config.DB_USER,
            "-d", config.DB_NAME,
            "--no-owner",
            "--no-privileges",
            "-f", sql_file
        ],
        capture_output=True,
        text=True,
        env=env
    )

    if result.returncode != 0:
        print(f"  ✗ pg_dump failed: {result.stderr}")
        return None, None
    
    sql_size = os.path.getsize(sql_file)
    print(f"  ✓ SQL dump created: {sql_size:,} bytes")

    # Compress with gzip
    print("  Compressing with gzip...")
    with open(sql_file, "rb") as f_in:
        with gzip.open(gz_file, "wb", compresslevel=9) as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_size = os.path.getsize(gz_file)
    ratio = (1 - gz_size / sql_size) * 100 if sql_size > 0 else 0
    print(f"  Compressed: {gz_size:,} bytes ({ratio:.1f}% reduction)")

    # Remove uncompressed file
    os.remove(sql_file)

    return gz_file, timestamp


def upload_to_s3(local_file, timestamp):
    """Upload the compressed backup to S3."""
    print("\n[3/5] Uploading to S3...")

    now = datetime.now()
    filename = os.path.basename(local_file)

    # Build the S3 key with date-based organization
    # Example: backups/postgres/2025/01/10/backup_20250110_143022.sql.gz
    s3_key = (
        f"{config.BACKUP_PREFIX}/"
        f"{now.year}/{now.month:02d}/{now.day:02d}/"
        f"{filename}"
    )

    s3 = boto3.client("s3", region_name=config.REGION)

    print(f"  Destination: s3://{config.BUCKET_NAME}/{s3_key}")

    # Upload the file
    s3.upload_file(
        Filename=local_file,
        Bucket=config.BUCKET_NAME,
        Key=s3_key,
        ExtraArgs={
            "Tagging": (
                f"RetentionDays={config.RETENTION_DAYS}"
                f"&BackupType=pg_dump"
                f"&Database={config.DB_NAME}"
            )
        }
    )

    # Verify upload
    try:
        response = s3.head_object(Bucket=config.BUCKET_NAME, Key=s3_key)
        size = response["ContentLength"]
        print(f"  ✓ Upload verified! Size in S3: {size:,} bytes")
    except ClientError:
        print("  ✗ Upload verification failed!")
        return None

    return s3_key


def apply_retention():
    """Delete backups older than RETENTION_DAYS."""
    print(f"\n[4/5] Applying retention policy ({config.RETENTION_DAYS} days)...")

    s3 = boto3.client("s3", region_name=config.REGION)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=config.RETENTION_DAYS)
    print(f"  Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")

    # List all backup objects
    deleted_count = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=config.BUCKET_NAME,
            Prefix=f"{config.BACKUP_PREFIX}/"
        )

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                last_modified = obj["LastModified"]

                if last_modified < cutoff_date:
                    print(f"  Deleting expired: {obj['Key']}")
                    s3.delete_object(
                        Bucket=config.BUCKET_NAME,
                        Key=obj["Key"]
                    )
                    deleted_count += 1

    except ClientError as e:
        print(f"  Warning: Could not list objects: {e}")

    if deleted_count == 0:
        print("  No expired backups found (all backups are recent)")
    else:
        print(f"  Deleted {deleted_count} expired backup(s)")


def cleanup_local(local_file):
    """Remove the local backup file."""
    print("\n[5/5] Cleaning up local files...")

    if local_file and os.path.exists(local_file):
        os.remove(local_file)
        print(f"  Removed: {local_file}")

    # Remove temp directory if empty
    backup_dir = os.path.dirname(local_file) if local_file else None
    if backup_dir and os.path.exists(backup_dir) and not os.listdir(backup_dir):
        os.rmdir(backup_dir)
        print(f"   Removed empty directory: {backup_dir}")


def list_backups():
    """Show all backups currently in S3."""
    print("\n Current backups in S3:")

    s3 = boto3.client("s3", region_name=config.REGION)

    try:
        response = s3.list_objects_v2(
            Bucket=config.BUCKET_NAME,
            Prefix=f"{config.BACKUP_PREFIX}/"
        )

        if "Contents" not in response:
            print("  (no backups found)")
            return

        for obj in response["Contents"]:
            size = obj.get("Size")
            size_kb = size / 1024
            modified = obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
            print(f"  {obj['Key']}")
            print(f"    Size: {size_kb:.1f} KB | Modified: {modified}")

    except ClientError as e:
        print(f"  Error: {e}")


# ── Main ──
if __name__ == "__main__":
    print("=" * 55)
    print("  PostgreSQL Backup to S3")
    print("=" * 55)
    print()

    # Step 1: Check prerequisites
    if not check_prerequisites():
        print("\n Prerequisites check failed. Fix issues above.")
        exit(1)

    # Step 2: Take backup
    local_file, timestamp = take_backup()
    if not local_file:
        print("\n Backup failed.")
        exit(1)

    # Step 3: Upload to S3
    s3_key = upload_to_s3(local_file, timestamp)
    if not s3_key:
        print("\n Upload failed.")
        exit(1)

    # Step 4: Apply retention
    apply_retention()

    # Step 5: Cleanup local
    cleanup_local(local_file)

    # Show summary
    list_backups()

    print()
    print("=" * 55)
    print("  BACKUP COMPLETE!")
    print(f"   s3://{config.BUCKET_NAME}/{s3_key}")
    print(f"   Retention: {config.RETENTION_DAYS} days")
    print("=" * 55)