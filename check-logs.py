import os
import boto3

def download_spark_logs(bucket_name, prefix, local_dir, access_key, secret_key, endpoint):
    """
    Download all Spark log files from a COS folder to a local directory.

    :param bucket_name: COS bucket name
    :param prefix: Path to logs folder in the bucket
    :param local_dir: Local directory to save files
    :param access_key: COS access key
    :param secret_key: COS secret key
    :param endpoint: COS endpoint URL
    """
    # Create S3 client for IBM COS
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint
    )

    # Make sure local directory exists
    os.makedirs(local_dir, exist_ok=True)

    # List all objects under the prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        print("No log files found.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        # Skip folder keys
        if key.endswith("/"):
            continue

        local_file_path = os.path.join(local_dir, os.path.basename(key))
        print(f"Downloading {key} -> {local_file_path}")
        s3.download_file(bucket_name, key, local_file_path)

    print("All log files downloaded successfully.")
    

bucket = "wastonx-data-bucket"
prefix = "spark/spark644/logs/9fef69c7-8b6d-40be-bcc7-8e62011217f9/"
local_dir = "./spark_logs"  # folder to save logs locally
access_key = "b302ecc0fad94af0a11522b17c47f982"
secret_key = "2f356ed16e98a7a0036b3dec8b16adaa8a7fb11d0cfa4061"
endpoint = "https://s3.us-east.cloud-object-storage.appdomain.cloud"

download_spark_logs(bucket, prefix, local_dir, access_key, secret_key, endpoint)
