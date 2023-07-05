import calendar
from somda_project.console import console
import os
import requests
import time
import urllib
import re
from contextlib import ExitStack
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


hour_mapping = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
    "E": 4,
    "F": 5,
    "G": 6,
    "H": 7,
    "I": 8,
    "J": 9,
    "K": 10,
    "L": 11,
    "M": 12,
    "N": 13,
    "O": 14,
    "P": 15,
    "Q": 16,
    "R": 17,
    "S": 18,
    "T": 19,
    "U": 20,
    "V": 21,
    "W": 22,
    "X": 23,
}


def decipher_hours(hourly_counts):
    hour_dict = {}
    for hour_char in hour_mapping:
        hour_dict[hour_mapping[hour_char]] = 0
    while hourly_counts:
        hour_char = hourly_counts[0]
        hour = hour_mapping[hour_char]

        visits = ""
        for char in hourly_counts[1:]:
            if char.isdigit():
                visits += char
            else:
                break

        hour_dict[hour] = int(visits)
        hourly_counts = hourly_counts[len(str(visits)) + 1 :]

    return hour_dict


def process_data_line(line):
    parts = line.strip().split(" ")
    wikicode = urllib.parse.unquote(parts[0])
    article_title = urllib.parse.unquote(parts[1])
    daily_total = int(parts[-2])
    hourly_counts = parts[-1]
    dct = {
        "wikicode": wikicode,
        "article_title": article_title,
        "daily_total": daily_total,
        "hourly_counts": hourly_counts,
    }
    # keep only stuff from wikipedia project and only articles (they don't start with a namespace)
    if "wikipedia" in dct["wikicode"] and not re.match(r"^\w+:", dct["article_title"]):
        return dct


def bz2_to_parquet(input_filepath, id_):
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

    return output_file

    console.log(f"Conversion time for {id_}: {round(time.time() - start_time, 2)} seconds")
