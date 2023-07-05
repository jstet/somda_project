import calendar
from somda_project.console import console
import os
import requests
import time
import urllib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import bz2


def gen_urls_old_wiki():
    base_url = "https://dumps.wikimedia.org/other/pageview_complete/"

    start_year = 2013
    end_year = 2015
    urls = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > 6:
                break
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                url = f"{base_url}{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2"
                urls.append({"url": url, "id": f"{year}{month:02d}{day:02d}"})

    return urls


def download_file(url):
    console.log(f"Starting download for {url['id']}")
    with requests.get(url["url"], stream=True) as raw:
        total_length = int(raw.headers.get("Content-Length"))
        filepath = f"temp_{os.path.basename(url['id'])}.bz2"
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
                    console.log(f"Progress: {progress:.2f}%, Speed: {speed:.2f} B/s")
                    update_time += 10
    return filepath, url["id"]


def to_parquet(input_filepath, id):
    # Define chunk size (adjust as needed)
    chunk_size = 100000

    # Define input and output file paths
    output_file = f"output_file_{id}.parquet"

    # Define Parquet schema
    parquet_schema = pa.schema(
        [
            ("wiki_code", pa.string()),
            ("article_title", pa.string()),
            ("daily_total", pa.int64()),
            ("hourly_counts", pa.string()),
        ]
    )

    print(f"Starting conversion for {id}")
    start_time = time.time()
    last_update_time = start_time

    # Open the input file in chunks
    with bz2.open(input_filepath, "rt") as file:
        # Create an empty Parquet file writer
        parquet_writer = None
        # Read and process the file in chunks
        while True:
            # Read a chunk of data
            chunk = []
            for _ in range(chunk_size):
                line = file.readline()
                if not line:
                    break
                parts = line.strip().split(" ")
                wikicode = urllib.parse.unquote(parts[0])
                article_title = urllib.parse.unquote(parts[1])

                daily_total = int(parts[-2])
                hourly_counts = " ".join(parts[-1])

                chunk.append((wikicode, article_title, daily_total, hourly_counts))

            # Break the loop if no more data is available
            if not chunk:
                break

            # Convert the chunk to a Pandas DataFrame
            df = pd.DataFrame(chunk, columns=["wiki_code", "article_title", "daily_total", "hourly_counts"])

            # Convert the DataFrame to a PyArrow table
            table = pa.Table.from_pandas(df, schema=parquet_schema)
            print(df.iloc[0])

            # Initialize the Parquet file writer if it hasn't been created
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(output_file, parquet_schema)

            # Write the chunk to the Parquet file
            parquet_writer.write_table(table)

            # Print progress every 10 seconds
            current_time = time.time()
            elapsed_time = current_time - last_update_time
            if elapsed_time >= 10:
                last_update_time = current_time
                file_position = file.tell()
                print(f"Current position in uncompressed file: {round(file_position / (1024 ** 3), 2)} GB")

        # Close the Parquet file writer
        if parquet_writer is not None:
            parquet_writer.close()

    print(f"File converted to Parquet: {output_file}")
    print(f"Conversion time: {round(time.time() - start_time, 2)} seconds")
