from somda_project.IO_handlers import upload_file, delete_object, check_object_exists, create_minio_client
import importlib_resources
import os
from dotenv import load_dotenv

load_dotenv()

endpoint = os.environ.get("BUCKET_ENDPOINT")
bucket_id = os.environ.get("BUCKET_ID")
access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
secret_key = os.environ.get("BUCKET_SECRET_KEY")
region = os.environ.get("BUCKET_REGION")


def test_upload_file():
    client = create_minio_client(endpoint, access_key, secret_key, region)
    resource_path = importlib_resources.files("tests.data").joinpath("foo.txt")
    path = str(resource_path)
    s3_path = upload_file(client, "test.parquet", path, bucket_id)
    assert check_object_exists(client, s3_path, bucket_id)
    delete_object(client, s3_path, bucket_id)
