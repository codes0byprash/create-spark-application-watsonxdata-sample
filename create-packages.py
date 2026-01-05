import os
import zipfile
import requests
import boto3
import base64
from config import get_config

BUCKET_NAME = get_config("BUCKET_NAME", required=True)
COS_ENDPOINT = get_config("COS_ENDPOINT", required=True)
COS_ACCESS_KEY = get_config("COS_ACCESS_KEY", required=True)
COS_SECRET_KEY = get_config("COS_SECRET_KEY", required=True)
INSTANCE_ROUTE = get_config("INSTANCE_ROUTE", required=True)

def prepare_package():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    required_files = ["config.py", "transform.py"]

    missing_files = [f for f in required_files if not os.path.exists(os.path.join(base_dir, f))]
    if missing_files:
        raise FileNotFoundError(f"Required files missing: {', '.join(missing_files)}")

    zip_name = os.path.join(base_dir, "dependencies.zip")
    # Overwrite if exists
    if os.path.exists(zip_name):
        os.remove(zip_name)

    with zipfile.ZipFile(zip_name, "w") as z:
        for f in required_files:
            z.write(os.path.join(base_dir, f), arcname=f)

    return zip_name


def upload_to_cos(file_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=COS_ACCESS_KEY,
        aws_secret_access_key=COS_SECRET_KEY,
        endpoint_url=COS_ENDPOINT,
    )

    object_name = os.path.basename(file_path)
    s3.upload_file(file_path, BUCKET_NAME, object_name)
    print(f"Uploaded {file_path} to bucket {BUCKET_NAME} as {object_name}")

if __name__ == "__main__":
        zip_file = prepare_package()
        upload_to_cos("main.py")
        upload_to_cos(zip_file)
