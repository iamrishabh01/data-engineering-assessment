

import boto3
from botocore.exceptions import ClientError
import config


def create_bucket():
    """Create S3 bucket if it doesn't exist."""
    print(f"Creating S3 bucket: {config.BUCKET_NAME}")
    print(f"Region: {config.REGION}")
    print()

    s3 = boto3.client("s3", region_name=config.REGION)

    try:
        
        if config.REGION == "us-east-1":
            s3.create_bucket(Bucket=config.BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=config.BUCKET_NAME,
                CreateBucketConfiguration={
                    "LocationConstraint": config.REGION
                }
            )
        print(f"   Bucket '{config.BUCKET_NAME}' created successfully!")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "BucketAlreadyOwnedByYou":
            print(f"   Bucket '{config.BUCKET_NAME}' already exists (you own it)")
        elif error_code == "BucketAlreadyExists":
            print(f"   Bucket name '{config.BUCKET_NAME}' is taken by someone else!")
            print("     Change BUCKET_NAME in config.py to something unique")
            return False
        else:
            raise e

    # Verify by listing buckets
    response = s3.list_buckets()
    bucket_names = [b["Name"] for b in response["Buckets"]]

    if config.BUCKET_NAME in bucket_names:
        print(f"  Verified: bucket exists in your account")
    else:
        print(f"   Bucket not found after creation!")
        return False

    return True


if __name__ == "__main__":
    print("=" * 50)
    print("Creating S3 Bucket")
    print("=" * 50)
    print()
    success = create_bucket()
    print()
    if success:
        print(" Bucket is ready! You can now run backup and data lake scripts.")
    else:
        print(" Failed. Fix the issue above and try again.")