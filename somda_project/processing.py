import duckdb
from somda_project.data import eu_elections
from somda_project.helpers import decipher_hours
import pandas as pd


def extract_page(input_filepath, wikicode, page_name):
    temp = duckdb.query(f"""
    SELECT *
    FROM '{input_filepath}'
    WHERE article_title LIKE '{page_name}' AND wikicode = '{wikicode}'
    """).fetchall()
    if temp:
        # return page with highest daily views
        temp = max(temp, key=lambda x: x[2])
        return temp
    else:
        return tuple()


def explode_timeseries(df):
    df = df.join(pd.json_normalize(df["hourly_views"]))
    df = df.drop("hourly_views", axis=1)

    df = df.melt(id_vars=["date", "wikicode"], var_name="hour", value_name="hourly_views")

    df["timestamp"] = pd.to_datetime(df["date"], format="%Y_%m_%d") + pd.to_timedelta(df["hour"].astype(int), unit="h")
    df = df.drop(["date", "hour"], axis=1)
    return df


def extract_election_page_timeseries(input_filepath, id_, year):
    election_page_lst = []
    for key, value in eu_elections.items():
        election_page = list(extract_page(input_filepath, value["wikicode"], value[year]["article_name"]))
        if election_page:
            row = [id_, value["wikicode"], decipher_hours(election_page[3])]
        else:
            row = [id_, value["wikicode"], {}]
        election_page_lst.append(row)

    df = pd.DataFrame(election_page_lst, columns=["date", "wikicode", "hourly_views"])
    df = explode_timeseries(df)
    df["hourly_views"] = df["hourly_views"].astype("Int64")
    output = f"{id_}.csv"
    df.to_csv(output, index=False)
    return output
