from somda_project.processing import extract_page, extract_election_page_timeseries, explode_timeseries
from somda_project.data import eu_elections, add_turnout
import importlib_resources
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

endpoint = os.environ.get("BUCKET_ENDPOINT")
bucket_id = os.environ.get("BUCKET_ID")
access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
secret_key = os.environ.get("BUCKET_SECRET_KEY")
region = os.environ.get("BUCKET_REGION")


def test_extract_page():
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("2014_05_19.parquet")
    path = str(resource_path)  # Convert the Path object to a string
    temp = extract_page(path, "de.wikipedia", "Europawahl_2014")
    assert temp == (
        "de.wikipedia",
        "Europawahl_2014",
        12183,
        "A38B25C18D32E84F281G807H809I1056J939K884L789M722N673O687P557Q585R594S635T633U739V347W179X70",
    )
    temp = extract_page(path, "bg.wikipedia", "Избори_за_Европейския_парламент_(2014)")
    print(temp)


def test_explode_timeseries():
    data = {
        "date": ["2014_05_16", "2014_05_16"],
        "wikicode": ["nl.wikipedia", "nl.wikipedia"],
        "hourly_views": [{0: 3, 1: 0, 2: 2, 3: 3}, {0: 2, 1: 0, 2: 0, 3: 0}],
    }
    df = pd.DataFrame(data)

    result = explode_timeseries(df)

    expected_columns = ["wikicode", "hourly_views", "timestamp"]
    expected_data = [
        ["nl.wikipedia", 3, pd.Timestamp("2014-05-16 00:00:00")],
        ["nl.wikipedia", 0, pd.Timestamp("2014-05-16 01:00:00")],
        ["nl.wikipedia", 2, pd.Timestamp("2014-05-16 02:00:00")],
        ["nl.wikipedia", 3, pd.Timestamp("2014-05-16 03:00:00")],
        ["nl.wikipedia", 2, pd.Timestamp("2014-05-16 00:00:00")],
        ["nl.wikipedia", 0, pd.Timestamp("2014-05-16 01:00:00")],
        ["nl.wikipedia", 0, pd.Timestamp("2014-05-16 02:00:00")],
        ["nl.wikipedia", 0, pd.Timestamp("2014-05-16 03:00:00")],
    ]
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    pd.testing.assert_frame_equal(
        result.sort_values("timestamp").reset_index(drop=True),
        expected_df.sort_values("timestamp").reset_index(drop=True),
    )


def test_extract_election_page_timeseries():
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("2014_05_19.parquet")
    path = str(resource_path)  # Convert the Path object to a string
    path = extract_election_page_timeseries(path, "2014_05_16", 2014)
    assert isinstance(path, str)
    os.remove(path)


def test_add_turnour():
    print(add_turnout(eu_elections))
