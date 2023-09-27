from somda_project.sql_queries import extract_page
from somda_project.data import hour_mapping
from typing import Dict
import pandas as pd
from somda_project.console import console


def process_election_page_timeseries(input_filepath: str, id_: str, year: int, eu_elections: dict) -> str:
    """
    Extracts election page timeseries from the input file, explodes the timeseries,
    saves it as a CSV file, and returns the output file path.

    Args:
        input_filepath (str): The filepath of the input file.
        id_ (str): The identifier associated with the data.
        year (int): The election year.

    Returns:
        str: The filepath of the output CSV file.

    Note:
        - This function iterates over the eu_elections dictionary to extract page timeseries
          for each specified election.
        - It uses the extract_page function to retrieve page information from the input file.
        - The extracted information is processed and organized into a DataFrame.
        - The DataFrame is exploded using the explode_timeseries function.
        - The exploded DataFrame is saved as a CSV file.
        - The output CSV file path is returned.
    """
    election_page_lst = []
    for key, value in eu_elections.items():
        if year == 2009:
            election_page = list(
                extract_page(input_filepath, value["wikicode"].replace(".wikipedia", ""), value[year]["article_name"])
            )
        else:
            election_page = list(extract_page(input_filepath, value["wikicode"], value[year]["article_name"]))
        if election_page:
            if year == 2009:
                row = [id_, value["wikicode"], election_page[2]]
            else:
                row = [id_, value["wikicode"], decipher_hours(election_page[2])]
            election_page_lst.append(row)
        else:
            console.log(f"Could not find page: {value[year]['article_name']} for in {value['wikicode']}")

    df = pd.DataFrame(election_page_lst, columns=["date", "wikicode", "hourly_views"])

    df = explode_timeseries(df, year)
    df["hourly_views"] = df["hourly_views"].astype("Int64")
    output = f"{id_}.csv"
    df.to_csv(output, index=False)
    return output


def explode_timeseries(df: pd.DataFrame, year) -> pd.DataFrame:
    """
    Explodes the hourly views timeseries in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the hourly views timeseries.

    Returns:
        pd.DataFrame: The exploded DataFrame.

    Note:
        - This function expands the hourly views timeseries in the DataFrame,
          transforming it from a nested structure into separate rows.
        - The resulting DataFrame includes columns for date, wikicode, hour, hourly_views,
          and timestamp (calculated from date and hour).
        - The original "date" and "hour" columns are dropped from the DataFrame.
    """
    if year != 2009:
        df = df.join(pd.json_normalize(df["hourly_views"]))
        df = df.drop("hourly_views", axis=1)

        df = df.melt(id_vars=["date", "wikicode"], var_name="hour", value_name="hourly_views")
        df["timestamp"] = pd.to_datetime(df["date"], format="%Y_%m_%d") + pd.to_timedelta(
            df["hour"].astype(int), unit="h"
        )
        df = df.drop(["date", "hour"], axis=1)
    else:
        df["timestamp"] = pd.to_datetime(df["date"], format="%Y_%m_%d_%H")
        df = df.drop(["date"], axis=1)
    return df


def decipher_hours(hourly_counts: str) -> Dict[int, int]:
    """
    Deciphers the hourly counts from a given string and returns a dictionary with hour-wise visit counts.

    Args:
        hourly_counts (str): A string representing the hourly counts. Example: "D1F1K1M1O1R1"

    Returns:
        Dict[int, int]: A dictionary containing the hour as the key and the corresponding visit count as the value.
    """
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
