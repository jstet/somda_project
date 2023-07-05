from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()


def create_minio_client(endpoint, access_key, secret_key, region):
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=True, region=region)
    return client


def upload_file(client, output_path, file_path, bucket_id):
    with open(file_path, "rb") as file:
        file_stat = os.stat(file_path)
        file_size = file_stat.st_size
        client.put_object(bucket_id, output_path, file, file_size)
    return output_path


def delete_object(client, object_key, bucket_id):
    client.remove_object(bucket_id, object_key)
    print(f"Object '{object_key}' deleted successfully from bucket '{bucket_id}'.")


def check_object_exists(client, object_key, bucket_id):
    try:
        client.stat_object(bucket_id, object_key)
        return True
    except Exception:
        return False
