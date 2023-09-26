import duckdb
from somda_project.data import hour_mapping
import urllib
import re
from typing import Optional, Dict
import pandas as pd
import copy
from somda_project.console import console


def get_turnout(df: pd.DataFrame, eu_elections: dict) -> dict:
    """
    Extracts turnout data from a DataFrame and updates a copy of the eu_elections dictionary.

    Args:
        df (pd.DataFrame): The DataFrame containing turnout data.
        eu_elections (dict): The eu_elections dictionary.

    Returns:
        dict: A copy of the eu_elections dictionary with updated turnout data.

    Note:
        - This function creates a deep copy of the eu_elections dictionary to avoid modifying the original dictionary.
        - The DataFrame is filtered based on the year and country ID.
    """
    eu_elections_copy = copy.deepcopy(eu_elections)

    for key, val in eu_elections_copy.items():
        print(key, val)
        print(df["COUNTRY_ID"].unique())
        val[2019]["turnout"] = df.loc[(df["YEAR"] == 2019) & (df["COUNTRY_ID"] == key), "RATE"].values[0]
        val[2014]["turnout"] = df.loc[(df["YEAR"] == 2014) & (df["COUNTRY_ID"] == key), "RATE"].values[0]

    return eu_elections_copy


def extract_page(input_filepath: str, wikicode: str, page_name: str, sec_search_term: str = None) -> tuple:
    """
    Extracts a specific page from the input file.

    Args:
        input_filepath (str): The filepath of the input file.
        wikicode (str): The wikicode of the page.
        page_name (str): The name of the page.

    Returns:
        tuple: The extracted page information as a tuple.

    Note:
        - This function queries the input file using the specified wikicode and page name.
        - If a matching page is found, it returns the page with the highest daily views.
        - Otherwise, it returns an empty tuple.
    """
    temp = duckdb.query(f"""
    SELECT *
    FROM '{input_filepath}'
    WHERE  contains(article_title, '{page_name}') AND wikicode = '{wikicode}'
   
    """).fetchall()
    if temp:
        # return page with highest daily views
        temp = max(temp, key=lambda x: x[2])
        return temp
    else:
        return tuple()


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


def extract_election_page_timeseries(input_filepath: str, id_: str, year: int, eu_elections: dict) -> str:
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


def process_data_line(line: str, year: int) -> Optional[dict]:
    """
    Processes a line of data and extracts relevant information.

    Args:
        line (str): A line of data to be processed.

    Returns:
        dict or None: A dictionary containing the extracted information if it meets the criteria,
                      or None if the line does not match the expected pattern.

    Note:
        The expected pattern for a valid line is:
        "<wikicode> <article_title> ... <daily_total> <hourly_counts>"

        The function performs the following steps:
        - Splits the line by spaces to extract individual parts.
        - Decodes URL-encoded parts using urllib.parse.unquote.
        - Extracts the wikicode, article_title, daily_total, and hourly_counts from the parts.
        - Constructs a dictionary with the extracted information.
        - Filters the dictionary to keep only data related to Wikipedia articles.
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
