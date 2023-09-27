import importlib.resources as importlib_resources
import os
import pyarrow.parquet as pq
from somda_project.converting import read_filter_data_line, compr_filter_to_parquet, parquet_schema


def validate_parquet_file(output_path, parquet_schema):
    assert os.path.isfile(output_path), f"Parquet file not found at {output_path}"

    parquet_file = pq.ParquetFile(output_path)
    assert parquet_file.metadata is not None, "Parquet file is not valid."

    table = parquet_file.read()
    assert set(table.column_names) == set(parquet_schema.names), "Parquet file does not have the expected columns."

    for column in parquet_schema.names:
        column_data = table[column]
        assert len(column_data) > 0, f"Column {column} is empty in the Parquet file."


def test_to_parquet():
    # Test for year 2009
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pagecounts-20090521-140000.gz")
    path = str(resource_path)
    output_path = compr_filter_to_parquet(path, "20090521-140000", 2009)
    validate_parquet_file(output_path, parquet_schema)
    os.remove(output_path)

    # Test for year 2014
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews-20140531-user.bz2")
    path = str(resource_path)
    output_path = compr_filter_to_parquet(path, "20140531", 2014)
    validate_parquet_file(output_path, parquet_schema)
    os.remove(output_path)

    # Test for year 2019
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews-20190526-user.bz2")
    path = str(resource_path)
    output_path = compr_filter_to_parquet(path, "20190526", 2019)
    validate_parquet_file(output_path, parquet_schema)
    os.remove(output_path)


def test_process_data_line():
    line = "als.wikipedia Spezial:ISBN-Suech/3883090387 desktop 1 L1"
    temp = read_filter_data_line(line, 20104)
    assert temp is None
    line = "als.wikipedia Darmstadtium desktop 1 I1"
    temp = read_filter_data_line(line, 2014)
    assert temp == {
        "wikicode": "als.wikipedia",
        "article_title": "Darmstadtium",
        "hourly_counts": "I1",
    }
    line = "en Article_Title 10"
    temp = read_filter_data_line(line, 2009)
    assert temp == {
        "wikicode": "en",
        "article_title": "Article_Title",
        "hourly_counts": "10",
    }
    line = "books.b Article_Title 10"
    temp = read_filter_data_line(line, 2009)
    assert temp is None
