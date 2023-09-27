from somda_project.helpers import gen_urls, get_env_vars
from somda_project.console import console
from somda_project.data import eu_elections
from somda_project.processing import extract_election_page_timeseries
from somda_project.IO_handlers import (
    create_minio_client,
    retrieve_file,
    upload_file,
    download_file,
    check_object_exists,
)
from somda_project.converting import compr_filter_to_parquet
from dotenv import load_dotenv
import modal
import pandas as pd
import os

load_dotenv()

image = modal.Image.debian_slim().poetry_install_from_file("pyproject.toml")

stub = modal.Stub(name="somda_project")


@stub.function(image=image, timeout=1500, secret=modal.Secret.from_dotenv(__file__), concurrency_limit=100)
def get_uncompress_to_parquet_(url):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    if not check_object_exists(client, f"{url['id']}.parquet", bucket_id):
        if url["year"] == 2009:
            compr_path, id_ = download_file(url, "gz")
        else:
            compr_path, id_ = download_file(url)
        output_filepath = compr_filter_to_parquet(compr_path, id_, url["year"])
        os.remove(compr_path)
        s3_path = upload_file(client, f"{id_}.parquet", output_filepath, bucket_id)
        os.remove(output_filepath)
        return s3_path


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def get_timeseries_day_(url):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    id_ = url["id"]
    year = url["year"]
    console.log(f"Retrieving parquet file for {id_}")
    parquet = retrieve_file(client, f"{id_}.parquet", bucket_id)
    console.log(f"Extracting election page views for {id_}")
    output_csv = extract_election_page_timeseries(parquet, id_, year, eu_elections)
    os.remove(parquet)
    console.log(f"Uploading csv for {id_}")
    s3_path = upload_file(client, output_csv, output_csv, bucket_id)
    os.remove(output_csv)
    return s3_path


@stub.function(image=image, timeout=1000, secret=modal.Secret.from_dotenv(__file__))
def concat_csvs_(urls):
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    lst = []
    for url in urls:
        id_ = url["id"]
        console.log(f"Retrieving csv file for {id_}")
        csv = retrieve_file(client, f"{id_}.csv", bucket_id)
        df = pd.read_csv(csv)
        os.remove(csv)
        lst.append(df)
    merged_df = pd.concat(lst, ignore_index=True)
    merged_df["hourly_views"] = merged_df["hourly_views"].astype("Int64")
    output = "merged_election_pages.csv"
    merged_df.to_csv(output, index=False)
    s3_path = upload_file(client, output, output, bucket_id)
    return s3_path


@stub.function(image=image, timeout=1500)
def gen_urls_():
    return gen_urls()


@stub.local_entrypoint()
def main():
    endpoint, bucket_id, access_key, secret_key, region = get_env_vars(os.environ)
    client = create_minio_client(endpoint, access_key, secret_key, region)
    urls = gen_urls_.call()
    list(get_uncompress_to_parquet_.map(urls))
    list(get_timeseries_day_.map(urls))
    merged = concat_csvs_.call(urls)
    csv = retrieve_file(client, merged, bucket_id)
    os.replace(f"./{csv}", f"./data/{csv}")
