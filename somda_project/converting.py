from somda_project.console import console
from typing import Optional
import urllib
import re
import time
from contextlib import ExitStack
import pyarrow as pa
import pyarrow.parquet as pq
import bz2  # noqa: F401
import gzip  # noqa: F401


parquet_schema = pa.schema(
    [
        ("wikicode", pa.string()),
        ("article_title", pa.string()),
        ("hourly_counts", pa.string()),
    ]
)


def read_filter_data_line(line: str, year: int) -> Optional[dict]:
    """
    Reads a line of filter data and extracts relevant information based on the year parameter.

    Args:
        line (str): The line of filter data to be processed. It should be formatted as follows:
            - For year 2009: "<wikicode> <article_title> <hourly_counts>" see https://dumps.wikimedia.org/other/pagecounts-raw/
            - For other years: "<wikicode> <article_title> ... <hourly_counts>" see https://dumps.wikimedia.org/other/pageview_complete/readme.html
        year (int): The year for which the data is being extracted.

    Returns:
        Optional[dict]: A dictionary containing the extracted information. The dictionary has the following keys:
            - "wikicode" (str): The decoded value of <wikicode>.
            - "article_title" (str): The decoded value of <article_title>.
            - "hourly_counts" (str): The value of <hourly_counts> based on the year parameter.
            NOTE: in the year 2009, this is a number, in the other years its a string encoding hourly page views per day

        Returns None if the line is not formatted correctly.

    Raises:
        ValueError: If the line is not formatted correctly.

    Example:
        read_filter_data_line("en.wikipedia Article_Title 10", 2010)
    """

    try:
        parts = line.strip().split(" ")
        wikicode = urllib.parse.unquote(parts[0])
        article_title = urllib.parse.unquote(parts[1])
        if year == 2009:
            hourly_counts = parts[2]
        else:
            hourly_counts = parts[-1]
        dct = {
            "wikicode": wikicode,
            "article_title": article_title,
            "hourly_counts": hourly_counts,
        }

        if year != 2009:
            # keep only stuff from wikipedia project and only articles (they don't start with a namespace)
            if "wikipedia" in dct["wikicode"] and not re.match(r"^\w+:", dct["article_title"]):
                return dct
        else:
            if "." not in dct["wikicode"] and not re.match(r"^\w+:", dct["article_title"]):
                return dct
    # sometimes its a line like: "de.wikipedia Versuchstelle_Gottow"
    except ValueError as e:
        print(e)
        console.log(e)
        console.log(line)
        return None


def compr_filter_to_parquet(input_filepath: str, id_: str, year: int) -> str:
    """
    Convert a compressed file to the Parquet format.

    Args:
        input_filepath (str): The path to the input file.
        id_ (str): The ID of the file.
        year (int): The year of the file.

    Returns:
        str: The name of the output Parquet file.
    """
    chunk_size = 100000
    output_file = f"{id_}.parquet"

    console.log(f"Starting conversion for {id_}")
    start_time = time.time()
    last_update_time = start_time
    if year == 2009:
        filetype = "gzip"
    else:
        filetype = "bz2"

    with eval(filetype).open(input_filepath, "rt") as file, ExitStack() as stack:
        parquet_writer = None
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = file.readline()

                if not line:
                    break

                dct = read_filter_data_line(line, year)

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
