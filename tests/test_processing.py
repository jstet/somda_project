from somda_project.processing import (
    explode_timeseries,
    decipher_hours,
)

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


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


def test_explode_timeseries():
    data = {
        "date": ["2014_05_16", "2014_05_16"],
        "wikicode": ["nl.wikipedia", "nl.wikipedia"],
        "hourly_views": [{0: 3, 1: 0, 2: 2, 3: 3}, {0: 2, 1: 0, 2: 0, 3: 0}],
    }
    df = pd.DataFrame(data)

    result = explode_timeseries(df, 2014)

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

    # for 2009
    data = {
        "date": ["2009_05_16_11"],
        "wikicode": ["nl"],
        "hourly_views": [6],
    }
    df = pd.DataFrame(data)

    result = explode_timeseries(df, 2009)

    print("RESULT", result)

    expected_columns = ["wikicode", "hourly_views", "timestamp"]
    expected_data = [["nl", 6, pd.Timestamp("2009-05-16 11:00:00")]]
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    pd.testing.assert_frame_equal(
        result.sort_values("timestamp").reset_index(drop=True),
        expected_df.sort_values("timestamp").reset_index(drop=True),
    )
