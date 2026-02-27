
import os
import time
import shutil
import boto3
from botocore.exceptions import ClientError
import config


def drop_athena_resources():
    """Drop all Athena tables and the database."""
    print("[1/4] Dropping Athena tables and database...")

    athena = boto3.client("athena", region_name=config.REGION)
    db = config.ATHENA_DATABASE
    output_location = f"s3://{config.BUCKET_NAME}/athena-results/"

    # Drop tables
    for table in ["orders", "events"]:
        try:
            athena.start_query_execution(
                QueryString=f"DROP TABLE IF EXISTS {db}.{table};",
                ResultConfiguration={"OutputLocation": output_location}
            )
            print(f"  Dropped table: {db}.{table}")
            time.sleep(3)
        except Exception as e:
            print(f"   Could not drop {table}: {e}")

    # Drop database
    try:
        athena.start_query_execution(
            QueryString=f"DROP DATABASE IF EXISTS {db};",
            ResultConfiguration={"OutputLocation": output_location}
        )
        print(f"   Dropped database: {db}")
        time.sleep(3)
    except Exception as e:
        print(f"  Could not drop database: {e}")

    print()


def delete_all_s3_objects():
    """Delete every object in the S3 bucket."""
    print("[2/4] Deleting all objects in S3...")

    s3 = boto3.resource("s3", region_name=config.REGION)

    try:
        bucket = s3.Bucket(config.BUCKET_NAME)
        objects = list(bucket.objects.all())

        if not objects:
            print("  Bucket is already empty")
        else:
            # Delete in batches of 1000 (S3 limit)
            deleted = 0
            for obj in objects:
                obj.delete()
                deleted += 1
                if deleted % 10 == 0:
                    print(f"  Deleted {deleted} objects...")

            print(f"   Deleted {deleted} objects total")

    except ClientError as e:
        print(f"  Error: {e}")

    print()


def delete_s3_bucket():
    """Delete the S3 bucket itself."""
    print("[3/4] Deleting S3 bucket...")

    s3 = boto3.client("s3", region_name=config.REGION)

    try:
        s3.delete_bucket(Bucket=config.BUCKET_NAME)
        print(f"   Bucket '{config.BUCKET_NAME}' deleted")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            print(f"  Bucket already doesn't exist")
        elif error_code == "BucketNotEmpty":
            print(f"   Bucket is not empty! Running delete again...")
            delete_all_s3_objects()
            s3.delete_bucket(Bucket=config.BUCKET_NAME)
            print(f"  Bucket deleted on retry")
        else:
            print(f"   Error: {e}")

    print()


def cleanup_local_files():
    """Remove locally generated files."""
    print("[4/4] Cleaning up local files...")

    dirs_to_remove = ["output"]
    files_to_remove = []

    for dir_name in dirs_to_remove:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"  Removed directory: {dir_name}/")
        else:
            print(f"  {dir_name}/ doesn't exist (already clean)")

    # Clean backup temp directory
    backup_dir = os.path.join(os.path.expanduser("~"), "pg_backups_temp")
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
        print(f" Removed: {backup_dir}/")

    print()


def verify_cleanup():
    """Verify everything is cleaned up."""
    print("Verifying cleanup...")

    s3 = boto3.client("s3", region_name=config.REGION)

    # Check bucket doesn't exist
    try:
        s3.head_bucket(Bucket=config.BUCKET_NAME)
        print(f"  Bucket '{config.BUCKET_NAME}' still exists!")
    except ClientError:
        print(f"   Bucket '{config.BUCKET_NAME}' confirmed deleted")

    # Check local files
    if not os.path.exists("output"):
        print("  Local output/ directory confirmed deleted")
    else:
        print("   Local output/ directory still exists!")

    print()


if __name__ == "__main__":
    print("=" * 55)
    print("  LEANUP — Removing All AWS Resources")
    print("=" * 55)
    print()

    # Ask for confirmation
    print(f"This will DELETE:")
    print(f"  • S3 bucket: {config.BUCKET_NAME}")
    print(f"  • Athena database: {config.ATHENA_DATABASE}")
    print(f"  • All files in output/ directory")
    print()

    confirm = input("Type 'yes' to confirm: ").strip().lower()
    if confirm != "yes":
        print("Cancelled.")
        exit(0)

    print()

    drop_athena_resources()
    delete_all_s3_objects()
    delete_s3_bucket()
    cleanup_local_files()
    verify_cleanup()

    print("=" * 55)
    print("  CLEANUP COMPLETE!")
    print("=" * 55)
    print()
    print("  Now take a screenshot of your AWS Billing page:")
    print("  https://console.aws.amazon.com/billing/home")
    print()