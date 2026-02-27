
import os
import boto3
import config


def upload_directory(local_dir, s3_prefix):
    """Upload an entire directory to S3, preserving structure."""
    print(f"Uploading {local_dir}/ → s3://{config.BUCKET_NAME}/{s3_prefix}/")
    print()

    s3 = boto3.client("s3", region_name=config.REGION)
    uploaded_count = 0
    total_size = 0

    # Walk through all files in the directory
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            # Full local path
            local_path = os.path.join(root, filename)

            # Build S3 key by replacing local_dir with s3_prefix
            # Example: output/orders/year=2025/... → datalake/orders/year=2025/...
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = f"{s3_prefix}/{relative_path}"

            # Upload
            file_size = os.path.getsize(local_path)
            print(f"{s3_key} ({file_size:,} bytes)")

            s3.upload_file(
                Filename=local_path,
                Bucket=config.BUCKET_NAME,
                Key=s3_key
            )

            uploaded_count += 1
            total_size += file_size

    return uploaded_count, total_size


def verify_upload():
    """List all files in the datalake prefix to verify."""
    print("\n Files in S3:")

    s3 = boto3.client("s3", region_name=config.REGION)
    response = s3.list_objects_v2(
        Bucket=config.BUCKET_NAME,
        Prefix=f"{config.DATALAKE_PREFIX}/"
    )

    if "Contents" not in response:
        print("  (no files found)")
        return

    for obj in response["Contents"]:
        size = obj.get("Size")
        size_kb = size / 1024
        print(f"  {obj['Key']} ({size_kb:.1f} KB)")

if __name__ == "__main__":
    print("=" * 55)
    print("  Uploading Data Lake to S3")
    print("=" * 55)
    print()

    if not os.path.exists("output"):
        print("'output/' directory not found!")
        print(" Run generate_data.py first.")
        exit(1)

    count, size = upload_directory("output", config.DATALAKE_PREFIX)

    verify_upload()

    print()
    print("=" * 55)
    print(f"  Uploaded {count} files ({size:,} bytes total)")
    print("=" * 55)