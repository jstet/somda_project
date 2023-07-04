from minio import Minio
import os
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

BUCKET_ENDPOINT = os.environ.get("BUCKET_ENDPOINT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BUCKET_ACCESS_KEY = os.environ.get("BUCKET_ACCESS_KEY_ID")
BUCKET_SECRET_KEY = os.environ.get("BUCKET_SECRET_KEY")

client = Minio(BUCKET_ENDPOINT, access_key=BUCKET_ACCESS_KEY, secret_key=BUCKET_SECRET_KEY, secure=True)


def upload_file(output_path, file):
    client.put_object(BUCKET_NAME, output_path, BytesIO(file))
