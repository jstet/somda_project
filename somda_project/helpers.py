from somda_project.console import console
from somda_project.processing import process_data_line
import time
from contextlib import ExitStack
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict
from datetime import datetime, timedelta
import bz2


def gen_urls() -> List[Dict[str, object]]:
    """
    Generates a list of URLs for downloading pageview data related to specific elections.

    Returns:
        A list of dictionaries, each containing the following information for a specific URL:
        - 'url': The URL for downloading the pageview data.
        - 'id': The identifier for the specific date of the pageview data (in the format 'YYYY_MM_DD').
        - 'year': The year of the election associated with the pageview data.
    """
    base_url = "https://dumps.wikimedia.org/other/pageview_complete/"
    elections = [
        {"name": "Europawahl 2014", "start_date": (2014, 5, 22), "end_date": (2014, 5, 25)},
        {"name": "Europawahl 2019", "start_date": (2019, 5, 23), "end_date": (2019, 5, 26)},
    ]
    num_days_before = 14  # Number of days before the election to include
    num_days_after = 14  # Number of days after the election to include

    urls = []

    for election in elections:
        start_date = datetime(*election["start_date"])
        end_date = datetime(*election["end_date"])

        # Adjust start and end dates by the specified number of days
        start_date -= timedelta(days=num_days_before)
        end_date += timedelta(days=num_days_after)

        current_date = start_date
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            url = f"{base_url}{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2"
            urls.append({"url": url, "id": f"{year}_{month:02d}_{day:02d}", "year": election["start_date"][0]})
            current_date += timedelta(days=1)

    return urls


def bz2_to_parquet(input_filepath: str, id_: str) -> str:
    """
    Converts a BZ2 file to Parquet format, filtering and processing the data.

    Args:
        input_filepath (str): The path to the input BZ2 file.
        id_ (str): The identifier associated with the data.

    Returns:
        str: The path to the output Parquet file.

    Note:
        - This function reads the input BZ2 file, filters the data, and converts it to Parquet format.
        - The data is filtered using the `process_data_line` function.
        - The resulting Parquet file will have the following schema:
          - 'wikicode': string
          - 'article_title': string
          - 'daily_total': int64
          - 'hourly_counts': string
        - The output Parquet file will be named '<id>.parquet', where <id> is the provided identifier.
        - The progress and position in the uncompressed file will be logged every 10 seconds.
        - The conversion time will be logged upon completion.
    """
    chunk_size = 100000
    output_file = f"{id_}.parquet"

    parquet_schema = pa.schema(
        [
            ("wikicode", pa.string()),
            ("article_title", pa.string()),
            ("daily_total", pa.int64()),
            ("hourly_counts", pa.string()),
        ]
    )

    console.log(f"Starting conversion for {id_}")
    start_time = time.time()
    last_update_time = start_time

    with bz2.open(input_filepath, "rt") as file, ExitStack() as stack:
        parquet_writer = None
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = file.readline()
                if not line:
                    break

                dct = process_data_line(line)

                if dct is not None:
                    chunk.append(dct)

            if chunk:
                arrays = [pa.array([row[col] for row in chunk]) for col in chunk[0]]
                table = pa.Table.from_arrays(arrays, schema=parquet_schema)
                if parquet_writer is None:
                    parquet_writer = stack.enter_context(pq.ParquetWriter(output_file, parquet_schema))

                parquet_writer.write_table(table)

            if not line:
                break

            current_time = time.time()
            elapsed_time = current_time - last_update_time
            if elapsed_time >= 10:
                last_update_time = current_time
                file_position = file.tell()
                console.log(
                    f"Current position in uncompressed file with id {id_}: {round(file_position / (1024 ** 3), 2)} GB"
                )

    if parquet_writer is not None:
        parquet_writer.close()

    console.log(f"Conversion time for {id_}: {round(time.time() - start_time, 2)} seconds")

    return output_file


def get_env_vars(environ: dict) -> tuple:
    """
    Retrieves environment variables related to a bucket.

    Args:
        environ (dict): The environment variables dictionary.

    Returns:
        tuple: A tuple containing the following bucket-related information:
               - endpoint: The bucket endpoint.
               - bucket_id: The bucket ID.
               - access_key: The access key ID for the bucket.
               - secret_key: The secret access key for the bucket.
               - region: The region associated with the bucket.

    Note:
        The function retrieves the values of the following environment variables:
        - BUCKET_ENDPOINT: The endpoint for the bucket.
        - BUCKET_ID: The ID of the bucket.
        - BUCKET_ACCESS_KEY_ID: The access key ID for the bucket.
        - BUCKET_SECRET_KEY: The secret access key for the bucket.
        - BUCKET_REGION: The region associated with the bucket.
    """
    endpoint = environ.get("BUCKET_ENDPOINT")
    bucket_id = environ.get("BUCKET_ID")
    access_key = environ.get("BUCKET_ACCESS_KEY_ID")
    secret_key = environ.get("BUCKET_SECRET_KEY")
    region = environ.get("BUCKET_REGION")
    return endpoint, bucket_id, access_key, secret_key, region
