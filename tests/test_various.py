from somda_project.helpers import download_file, bz2_to_parquet, decipher_hours, process_data_line, gen_urls
from somda_project.s3_funcs import upload_file, delete_object, check_object_exists, create_minio_client
from somda_project.processing import process_parquet
import importlib_resources
import requests
import os
from dotenv import load_dotenv

load_dotenv()


def test_gen_urls():
    urls = gen_urls()
    print(len(urls))
    for url in urls:
        response = requests.head(url["url"])
        assert response.status_code == 200


def test_download_file():
    filepath, _id = download_file(
        {
            "url": "https://dumps.wikimedia.org/other/pageview_complete/2013/2013-01/pageviews-20130101-user.bz2",
            "id": "20130101",
        }
    )
    assert os.path.isfile(filepath)
    os.remove(filepath)


def test_decipher_hours():
    temp = decipher_hours("D1F1K1M1O1R1")
    assert temp == {
        0: 0,
        1: 0,
        2: 0,
        3: 1,
        4: 0,
        5: 1,
        6: 0,
        7: 0,
        8: 0,
        9: 0,
        10: 1,
        11: 0,
        12: 1,
        13: 0,
        14: 1,
        15: 0,
        16: 0,
        17: 1,
        18: 0,
        19: 0,
        20: 0,
        21: 0,
        22: 0,
        23: 0,
    }
    temp = decipher_hours("A1B1D2J1K1L1M1N1P1Q1S1T3V2W1X2")
    assert temp == {
        0: 1,
        1: 1,
        2: 0,
        3: 2,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 1,
        10: 1,
        11: 1,
        12: 1,
        13: 1,
        14: 0,
        15: 1,
        16: 1,
        17: 0,
        18: 1,
        19: 3,
        20: 0,
        21: 2,
        22: 1,
        23: 2,
    }


def test_process_data_line():
    line = "als.wikipedia Spezial:ISBN-Suech/3883090387 desktop 1 L1"
    temp = process_data_line(line)
    assert temp is None
    line = "als.wikipedia Darmstadtium desktop 1 I1"
    temp = process_data_line(line)
    assert temp == {
        "wikicode": "als.wikipedia",
        "article_title": "Darmstadtium",
        "daily_total": 1,
        "hourly_counts": "I1",
    }


def test_to_parquet_old():
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews.bz2")
    path = str(resource_path)  # Convert the Path object to a string
    print(path)
    output_path = bz2_to_parquet(path, "20130101")
    assert os.path.isfile(output_path)
    os.remove(output_path)


def test_to_parquet_new():
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews-new.bz2")
    path = str(resource_path)  # Convert the Path object to a string
    print(path)
    output_path = bz2_to_parquet(path, "20130101")
    assert os.path.isfile(output_path)
    os.remove(output_path)


def test_process_parquet():
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("pageviews.parquet")
    path = str(resource_path)  # Convert the Path object to a string
    process_parquet(path)


def test_upload_file():
    endpoint = os.environ.get("BUCKET_ENDPOINT")
    bucket_id = os.environ.get("BUCKET_ID")
    access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
    secret_key = os.environ.get("BUCKET_SECRET_KEY")
    region = os.environ.get("BUCKET_REGION")
    client = create_minio_client(endpoint, access_key, secret_key, region)
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("pageviews.parquet")
    path = str(resource_path)  # Convert the Path object to a string
    s3_path = upload_file(client, "test.parquet", path, bucket_id)
    assert check_object_exists(client, s3_path, bucket_id)
    delete_object(client, s3_path, bucket_id)
