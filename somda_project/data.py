import importlib_resources
import pandas as pd

eu_elections = {
    "BE": {
        "language": "dutch",
        "wikicode": "nl.wikipedia",
        2014: {"article_name": "Europese_Parlementsverkiezingen_2014", "turnout": 89.64},
        2019: {"article_name": "Europese_Parlementsverkiezingen_2019", "turnout": 88.47},
    },
    "BG": {
        "language": "bulgarian",
        "wikicode": "bg.wikipedia",
        2014: {"article_name": "Избори_за_Европейския_парламент_(2014)", "turnout": 35.84},
        2019: {"article_name": None, "turnout": 32.64},
    },
    "DK": {
        "language": "danish",
        "wikicode": "da.wikipedia",
        2014: {"article_name": "Europa-Parlamentsvalget_2014", "turnout": 56.32},
        2019: {"article_name": "Europa-Parlamentsvalget_2019", "turnout": 66.08},
    },
    "DE": {
        "language": "german",
        "wikicode": "de.wikipedia",
        2014: {"article_name": "Europawahl_2014", "turnout": 48.1},
        2019: {"article_name": "Europawahl_2019", "turnout": 61.38},
    },
    "FR": {
        "language": "french",
        "wikicode": "fr.wikipedia",
        2014: {"article_name": "Élections_européennes_de_2014", "turnout": 42.43},
        2019: {"article_name": "Élections_européennes_de_2019", "turnout": 50.12},
    },
}


def add_turnout(eu_elections):
    resource_path = importlib_resources.files("somda_project.csv").joinpath("turnout-country.csv")
    path = str(resource_path)  # Convert the Path object to a string
    turnout_2014 = pd.read_csv(path, delimiter=";")
    for key, val in eu_elections.items():
        val[2019]["turnout"] = turnout_2014[(turnout_2014["YEAR"] == 2019) & (turnout_2014["COUNTRY_ID"] == key)][
            "RATE"
        ].values[0]
        val[2014]["turnout"] = turnout_2014[(turnout_2014["YEAR"] == 2014) & (turnout_2014["COUNTRY_ID"] == key)][
            "RATE"
        ].values[0]
    return eu_elections
