from minio import Minio
from somda_project.console import console
import os
import time
import requests
from typing import Tuple


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


def retrieve_file(client, file_path, bucket_id):
    object_name = os.path.basename(file_path)
    file_info = client.stat_object(bucket_id, file_path)
    total_length = file_info.size
    download_path = os.path.join(os.getcwd(), object_name)
    with open(download_path, "wb") as file_data:
        response_stream = client.get_object(bucket_id, file_path)
        start_time = time.time()
        update_time = start_time + 10
        downloaded = 0
        for data in response_stream.stream(32 * 1024):
            file_data.write(data)
            downloaded += len(data)
            if time.time() >= update_time:
                elapsed_time = time.time() - start_time
                speed = downloaded / elapsed_time
                progress = min(downloaded / total_length, 1.0) * 100
                print(f"File: {object_name} Progress: {progress:.2f}%, Speed: {speed:.2f} B/s")
                update_time += 10
    return object_name


def download_file(url: dict, filetype: str = "bz2") -> Tuple[str, str]:
    """
    Downloads a file from the specified URL and saves it locally.

    Args:
        url (dict): Contains the url as string ("url") and associated id ("id") for the file to be downloaded.
        filetype (str, optional): The file type extension. Defaults to "bz2".

    Returns:
        Tuple[str, str]: A tuple containing the file path and the identifier of the downloaded file.
    """
    console.log(f"Starting download for {url['id']}")
    with requests.get(url["url"], stream=True) as raw:
        total_length = int(raw.headers.get("Content-Length"))
        filepath = f"temp_{os.path.basename(url['id'])}.{filetype}"
        with open(filepath, "wb") as output:
            start_time = time.time()
            update_time = start_time + 10
            for chunk in raw:
                output.write(chunk)
                if time.time() >= update_time:
                    elapsed_time = time.time() - start_time
                    downloaded = output.tell()
                    speed = downloaded / elapsed_time
                    progress = min(downloaded / total_length, 1.0) * 100
                    console.log(f"ID: {url['id']} Progress: {progress:.2f}%, Speed: {speed:.2f} B/s")
                    update_time += 10
    return filepath, url["id"]
