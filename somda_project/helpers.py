import requests
from typing import List, Dict
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
from somda_project.IO_handlers import download_file
from somda_project.data import eu_elections
import pandas as pd
import os


def gen_urls() -> List[Dict[str, object]]:
    """
    Generates a list of URLs for downloading pageview data related to specific elections.

    Returns:
        A list of dictionaries, each containing the following information for a specific URL:
        - 'url': The URL for downloading the pageview data.
        - 'id': The identifier for the specific date of the pageview data (in the format 'YYYY_MM_DD').
        - 'year': The year of the election associated with the pageview data.
    """
    elections = [
        {"name": "Europawahl 2009", "start_date": (2009, 6, 4), "end_date": (2009, 6, 7)},
        {"name": "Europawahl 2014", "start_date": (2014, 5, 22), "end_date": (2014, 5, 25)},
        {"name": "Europawahl 2019", "start_date": (2019, 5, 23), "end_date": (2019, 5, 26)},
    ]
    num_days_before = 14  # Number of days before the election to include
    num_days_after = 14  # Number of days after the election to include

    # retrieving list of links so that we dont have to make a request for each generated url to check for the random 01
    # sat end
    dct_2009 = {}
    for month in ["05", "06"]:
        url = f"https://dumps.wikimedia.org/other/pagecounts-raw/2009/2009-{month}/"  # Replace with your desired URL

        # Send a GET request to the URL and retrieve the HTML content
        response = requests.get(url)
        html_content = response.text

        # Parse the HTML content with Beautiful Soup
        soup = BeautifulSoup(html_content, "html.parser")

        # Find all the anchor tags (links)
        links = soup.find_all("a")

        # Extract the href attribute from each link and store them in a list
        link_list = [
            f"https://dumps.wikimedia.org/other/pagecounts-raw/2009/2009-{month}/{link.get('href')}" for link in links
        ]
        dct_2009[month] = link_list

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
            if year == 2009:
                base_url = "https://dumps.wikimedia.org/other/pagecounts-raw/2009/2009-"
                for hour in range(24):
                    url = f"{base_url}{month:02d}/pagecounts-2009{month:02d}{day:02d}-{hour:02d}0000.gz"
                    if url not in dct_2009[f"{month:02d}"]:
                        url = f"{base_url}{month:02d}/pagecounts-2009{month:02d}{day:02d}-{hour:02d}0001.gz"
                    urls.append(
                        {
                            "url": url,
                            "id": f"{year}_{month:02d}_{day:02d}_{hour:02d}",
                            "year": election["start_date"][0],
                        }
                    )
            else:
                base_url = "https://dumps.wikimedia.org/other/pageview_complete/"
                url = f"{base_url}{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2"
                urls.append({"url": url, "id": f"{year}_{month:02d}_{day:02d}", "year": election["start_date"][0]})
            current_date += timedelta(days=1)

    return urls


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


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def get_election_data() -> str:
    """
    Downloads the election data from the European Parliament website, processes it, and returns the processed data.

    Returns:
        dict: The processed election data.
    """
    turnout_url = "https://www.europarl.europa.eu/election-results-2019/data-sheets/csv/turnout/turnout-country.csv"
    output_filepath, id_ = download_file({"url": turnout_url, "id": "turnout"}, "csv")
    turnout_df = pd.read_csv(output_filepath, delimiter=";")
    os.remove(output_filepath)

    for key, val in eu_elections.items():
        val[2019]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2019) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]
        val[2014]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2014) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]
        val[2009]["turnout"] = turnout_df.loc[
            (turnout_df["YEAR"] == 2009) & (turnout_df["COUNTRY_ID"] == key), "RATE"
        ].values[0]

    return eu_elections


def lineplot(df, ax, show_inset=False, fig=None):
    ax.set_xlabel("")
    ax.set_ylabel("Normalized Daily Page Views")
    ax.margins(y=0)
    ax.plot(df)
    ax.tick_params(axis="x", labelrotation=45)

    if show_inset:
        left, bottom, width, height = [0.2, 0.6, 0.2, 0.2]
        ax2 = fig.add_axes([left, bottom, width, height])
        ax2.plot(df)
        ax2.set_yscale("log", base=10)
        ax2.set_xticklabels([])
        ax2.tick_params(labeltop=True, labelright=True)

    # Add color legend
    ax.legend(df.columns)
