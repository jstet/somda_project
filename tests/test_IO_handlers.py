from somda_project.IO_handlers import (
    upload_file,
    delete_object,
    check_object_exists,
    create_minio_client,
    download_file,
)
import importlib_resources
import os
from dotenv import load_dotenv

load_dotenv()

endpoint = os.environ.get("BUCKET_ENDPOINT")
bucket_id = os.environ.get("BUCKET_ID")
access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
secret_key = os.environ.get("BUCKET_SECRET_KEY")
region = os.environ.get("BUCKET_REGION")


def test_download_file():
    filepath, _id = download_file(
        {
            "url": "https://dumps.wikimedia.org/other/pageview_complete/2013/2013-01/pageviews-20130101-user.bz2",
            "id": "20130101",
        }
    )
    assert os.path.isfile(filepath)
    os.remove(filepath)
    filepath, _id = download_file(
        {
            "url": "https://dumps.wikimedia.org/other/pagecounts-raw/2009/2009-05/pagecounts-20090521-010000.gz",
            "id": "20090521-010000",
        },
        "gz",
    )
    assert os.path.isfile(filepath)
    os.remove(filepath)


def test_upload_file():
    client = create_minio_client(endpoint, access_key, secret_key, region)
    resource_path = importlib_resources.files("tests.data").joinpath("foo.txt")
    path = str(resource_path)
    s3_path = upload_file(client, "test.parquet", path, bucket_id)
    assert check_object_exists(client, s3_path, bucket_id)
    delete_object(client, s3_path, bucket_id)
