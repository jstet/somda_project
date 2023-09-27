from somda_project.helpers import gen_urls, get_env_vars
from somda_project.console import console
from somda_project.data import eu_elections
from somda_project.processing import decipher_hours, explode_timeseries
from somda_project.sql_queries import extract_page
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
import numpy as np

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

    election_page_lst = []

    for value in eu_elections.values():
        wikicode = value["wikicode"].replace(".wikipedia", "") if year == 2009 else value["wikicode"]
        article_names = value[year]["article_names"]
        temp_lst = []
        for article_name in article_names:
            election_page = list(extract_page(parquet, wikicode, article_name))

            if election_page:
                hourly_views = election_page[2] if year == 2009 else decipher_hours(election_page[2])
                summed_hourly_views = int(election_page[2]) if year == 2009 else sum(hourly_views.values())
                temp_lst.append({"row": [id_, wikicode, hourly_views], "sum": summed_hourly_views})

            else:
                console.log(f"Could not find page: {article_name} for in {wikicode}")
                temp_lst.append({"row": [id_, wikicode, np.nan], "sum": 0})
        election_page_dct = max(temp_lst, key=lambda x: x["sum"])
        election_page_lst.append(election_page_dct["row"])
    df = pd.DataFrame(election_page_lst, columns=["date", "wikicode", "hourly_views"])
    df = explode_timeseries(df, year)
    df["hourly_views"] = df["hourly_views"].astype("Int64")
    output_csv = f"{id_}.csv"
    df.to_csv(output_csv, index=False)

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
    # list(get_uncompress_to_parquet_.map(urls))
    # list(get_timeseries_day_.map(urls))
    merged = concat_csvs_.call(urls)
    csv = retrieve_file(client, merged, bucket_id)
    # in the column wikicode, remove .wikipedia
    df = pd.read_csv(csv)
    df["wikicode"] = df["wikicode"].str.replace(".wikipedia", "")
    df.to_csv("data/merged_election_pages.csv", index=False)
    os.remove(csv)
