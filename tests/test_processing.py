from somda_project.processing import (
    extract_page,
    extract_election_page_timeseries,
    explode_timeseries,
    get_turnout,
    get_parties,
    process_data_line,
    decipher_hours,
)
from somda_project.data import eu_elections
from somda_project.IO_handlers import download_file
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


def test_process_data_line():
    line = "als.wikipedia Spezial:ISBN-Suech/3883090387 desktop 1 L1"
    temp = process_data_line(line)
    assert temp is None
    line = "als.wikipedia Darmstadtium desktop 1 I1"
    temp = process_data_line(line)
    assert temp == {
        "wikicode": "als.wikipedia",
        "article_title": "Darmstadtium",
        "daily_total": 1,
        "hourly_counts": "I1",
    }


def test_decipher_hours():
    temp = decipher_hours("D1F1K1M1O1R1")
    assert temp == {
        0: 0,
        1: 0,
        2: 0,
        3: 1,
        4: 0,
        5: 1,
        6: 0,
        7: 0,
        8: 0,
        9: 0,
        10: 1,
        11: 0,
        12: 1,
        13: 0,
        14: 1,
        15: 0,
        16: 0,
        17: 1,
        18: 0,
        19: 0,
        20: 0,
        21: 0,
        22: 0,
        23: 0,
    }
    temp = decipher_hours("A1B1D2J1K1L1M1N1P1Q1S1T3V2W1X2")
    assert temp == {
        0: 1,
        1: 1,
        2: 0,
        3: 2,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 1,
        10: 1,
        11: 1,
        12: 1,
        13: 1,
        14: 0,
        15: 1,
        16: 1,
        17: 0,
        18: 1,
        19: 3,
        20: 0,
        21: 2,
        22: 1,
        23: 2,
    }


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
    path = extract_election_page_timeseries(path, "2014_05_16", 2014, eu_elections)
    assert isinstance(path, str)
    os.remove(path)


def test_get_turnout():
    turnout_url = "https://www.europarl.europa.eu/election-results-2019/data-sheets/csv/turnout/turnout-country.csv"
    output_filepath, id_ = download_file({"url": turnout_url, "id": "turnout"}, "csv")
    turnout = pd.read_csv(output_filepath, delimiter=";")
    os.remove(output_filepath)
    eu_elections_proc = get_turnout(turnout, eu_elections)
    print(eu_elections_proc)


def test_get_parties():
    parties_2014_url = (
        "https://www.europarl.europa.eu/election-results-2019/data-sheets/csv/2014-2019/election-results/parties.csv"
    )
    output_filepath, id_ = download_file({"url": parties_2014_url, "id": "parties_2014"}, "csv")
    parties = pd.read_csv(output_filepath, delimiter=";")
    os.remove(output_filepath)
    eu_elections_proc = get_parties(parties, eu_elections, 2014)
    print(eu_elections_proc, "\n")

    parties_2019_url = (
        "https://www.europarl.europa.eu/election-results-2019/data-sheets/csv/2019-2024/election-results/parties.csv"
    )
    output_filepath, id_ = download_file({"url": parties_2019_url, "id": "parties_2019"}, "csv")
    parties = pd.read_csv(output_filepath, delimiter=";")
    os.remove(output_filepath)
    eu_elections_proc = get_parties(parties, eu_elections, 2019)
    print(eu_elections_proc)
