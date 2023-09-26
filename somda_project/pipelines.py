from somda_project.helpers import compr_to_parquet
from somda_project.IO_handlers import upload_file, check_object_exists, retrieve_file, download_file
from somda_project.processing import extract_election_page_timeseries
from somda_project.console import console
from somda_project.data import eu_elections
import pandas as pd
from typing import Any
import os


def get_election_data() -> str:
    """
    Downloads the election data from the European Parliament website, processes it, and returns the processed data.

    Returns:
        dict: The processed election data.
    """
    turnout_url = "https://www.europarl.europa.eu/election-results-2019/data-sheets/csv/turnout/turnout-country.csv"
    output_filepath, id_ = download_file({"url": turnout_url, "id": "turnout"}, "csv")
    turnout_df = pd.read_csv(output_filepath, delimiter=";")
    os.remove(output_filepath)

    for key, val in eu_elections.items():
        val[2019]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2019) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]
        val[2014]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2014) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]
        val[2009]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2009) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]

    return eu_elections


def get_upload_parquet(url: dict, minio_client: Any, bucket_id: str) -> str:
    """
    Downloads a file from the specified URL, converts it to Parquet format, and uploads it.

    Args:
        url (dict): The URL and associated information for the file to be downloaded.
        minio_client (Any): The MinIO client object.
        bucket_id (str): The ID of the bucket.

    Returns:
        str: The S3 path of the uploaded Parquet file.

    Note:
        - This function checks if the Parquet file already exists in the bucket.
        - If the file doesn't exist, it downloads the file from the URL using the download_file function.
        - The downloaded file is converted to the Parquet format using the compr_to_parquet function.
        - The original and converted files are deleted after processing.
        - The Parquet file is uploaded to the S3 server using the upload_file function.
    """
    if not check_object_exists(minio_client, f"{url['id']}.parquet", bucket_id):
        if url["year"] == 2009:
            compr_path, id_ = download_file(url, "gz")
        else:
            compr_path, id_ = download_file(url)
        output_filepath = compr_to_parquet(compr_path, id_, url["year"])
        os.remove(compr_path)
        s3_path = upload_file(minio_client, f"{id_}.parquet", output_filepath, bucket_id)
        os.remove(output_filepath)
        return s3_path


def get_timeseries_day(url: dict, minio_client: Any, bucket_id: str) -> str:
    """
    Retrieves a Parquet file from the S3 server, extracts election page timeseries,
    saves it as a CSV file, and uploads it back to the server.

    Args:
        url (dict): The URL and associated information for the Parquet file.
        minio_client (Any): The MinIO client object.
        bucket_id (str): The ID of the bucket.

    Returns:
        str: The S3 path of the uploaded CSV file.

    Note:
        - This function retrieves the Parquet file from the S3 server using the retrieve_file function.
        - It extracts the election page timeseries using the extract_election_page_timeseries function.
        - The extracted data is saved as a CSV file.
        - The Parquet file and the CSV file are deleted after processing.
        - The CSV file is uploaded to the S3 server using the upload_file function.
    """
    id_ = url["id"]
    year = url["year"]
    console.log(f"Retrieving parquet file for {id_}")
    parquet = retrieve_file(minio_client, f"{id_}.parquet", bucket_id)
    console.log(f"Extracting election page views for {id_}")
    output_csv = extract_election_page_timeseries(parquet, id_, year, eu_elections)
    os.remove(parquet)
    console.log(f"Uploading csv for {id_}")
    s3_path = upload_file(minio_client, output_csv, output_csv, bucket_id)
    os.remove(output_csv)
    return s3_path


def concat_csvs(urls: list, minio_client: Any, bucket_id: str) -> str:
    """
    Retrieves multiple CSV files from the S3 server, concatenates the data into a single DataFrame,
    and uploads the merged data as a CSV file.

    Args:
        urls (list): A list of dictionaries containing the URLs and associated information for the CSV files.
        minio_client (Any): The MinIO client object.
        bucket_id (str): The ID of the bucket.

    Returns:
        str: The S3 path of the uploaded merged CSV file.

    Note:
        - This function retrieves each CSV file from the S3 server using the retrieve_file function.
        - The retrieved CSV files are read into separate DataFrames.
        - The original CSV files are deleted after reading.
        - The DataFrames are concatenated into a single DataFrame using pd.concat.
        - The 'hourly_views' column in the merged DataFrame is converted to 'Int64' type.
        - The merged DataFrame is saved as a CSV file.
        - The CSV file is uploaded to the S3 server using the upload_file function.
    """
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
