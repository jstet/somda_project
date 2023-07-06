from somda_project.helpers import download_file, bz2_to_parquet
from somda_project.s3_funcs import upload_file, check_object_exists, retrieve_file
from somda_project.processing import extract_election_page_timeseries
from somda_project.console import console
import pandas as pd
import os


def get_upload_parquet(url, minio_client, bucket_id):
    if not check_object_exists(minio_client, f"{url['id']}.parquet", bucket_id):
        bz2_path, id_ = download_file(url)
        output_filepath = bz2_to_parquet(bz2_path, id_)
        os.remove(bz2_path)
        s3_path = upload_file(minio_client, f"{id_}.parquet", output_filepath, bucket_id)
        os.remove(output_filepath)
        return s3_path


def get_timeseries_day(url, minio_client, bucket_id):
    id_ = url["id"]
    year = url["year"]
    console.log(f"Retrieving parquet file for {id_}")
    parquet = retrieve_file(minio_client, f"{id_}.parquet", bucket_id)
    console.log(f"Extracting election page views for {id_}")
    output_csv = extract_election_page_timeseries(parquet, id_, year)
    os.remove(parquet)
    console.log(f"Uploading csv for {id_}")
    s3_path = upload_file(minio_client, output_csv, output_csv, bucket_id)
    os.remove(output_csv)
    return s3_path


def concat_csvs(urls, minio_client, bucket_id):
    lst = []
    for url in urls:
        id_ = url["id"]
        console.log(f"Retrieving csv file for {id_}")
        csv = retrieve_file(minio_client, f"{id_}.csv", bucket_id)
        df = pd.read_csv(csv)
        os.remove(csv)
        lst.append(df)
    merged_df = pd.concat(lst, ignore_index=True)
    merged_df["hourly_views"] = merged_df["hourly_views"].astype("Int64")
    output = "merged_election_pages.csv"
    merged_df.to_csv(output, index=False)
    s3_path = upload_file(minio_client, output, output, bucket_id)
    return s3_path
