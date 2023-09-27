from datetime import date

eu_elections = {
    "CZ": {
        "language": "czech",
        "wikicode": "cs.wikipedia",
        2009: {
            "election_date": date(2009, 6, 5),
            "article_names": ["Volby_do_Evropského_parlamentu_2009", "Volby_do_Evropského_parlamentu_v_Česku_2009"],
        },
        2014: {
            "election_date": date(2014, 5, 23),
            "article_names": ["Volby_do_Evropského_parlamentu_2014", "Volby_do_Evropského_parlamentu_v_Česku_2014"],
        },
        2019: {
            "election_date": date(2019, 5, 24),
            "article_names": ["Volby_do_Evropského_parlamentu_2019", "Volby_do_Evropského_parlamentu_v_Česku_2019"],
        },
    },
    "DK": {
        "language": "danish",
        "wikicode": "da.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": ["Europa-Parlamentsvalg_2009", "Europa-Parlamentsvalget_2009_i_Danmark"],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": ["Europa-Parlamentsvalg_2014", "Europa-Parlamentsvalget_2014_i_Danmark"],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": ["Europa-Parlamentsvalg_2019", "Europa-Parlamentsvalget_2019_i_Danmark"],
        },
    },
    "DE": {
        "language": "german",
        "wikicode": "de.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": ["Europawahl_2009", "Europawahl_2009_in_Deutschland"],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": ["Europawahl_2014", "Europawahl_2009_in_Deutschland"],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": ["Europawahl_2019", "Europawahl_2009_in_Deutschland"],
        },
    },
    "EL": {
        "language": "greek",
        "wikicode": "el.wikipedia",
        2009: {"election_date": date(2009, 6, 7), "article_names": ["Ευρωεκλογές_2009", "Ελληνικές_ευρωεκλογές_2009"]},
        2014: {"election_date": date(2014, 5, 25), "article_names": ["Ευρωεκλογές_2014", "Ελληνικές_ευρωεκλογές_2014"]},
        2019: {"election_date": date(2019, 5, 26), "article_names": ["Ευρωεκλογές_2019", "Ελληνικές_ευρωεκλογές_2019"]},
    },
    "FI": {
        "language": "finish",
        "wikicode": "fi.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": ["Europarlamenttivaalit_2009", "Suomen_europarlamenttivaalit_2009"],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": ["Europarlamenttivaalit_2014", "Suomen_europarlamenttivaalit_2014"],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": ["Europarlamenttivaalit_2019", "Suomen_europarlamenttivaalit_2019"],
        },
    },
    "FR": {
        "language": "french",
        "wikicode": "fr.wikipedia",
        2009: {
            "election_date": date(2009, 6, 6),
            "article_names": ["Élections_européennes_de_2009_en_France", "Élections_européennes_de_2009"],
        },
        2014: {
            "election_date": date(2014, 5, 24),
            "article_names": ["Élections_européennes_de_2014_en_France", "Élections_européennes_de_2014"],
        },
        2019: {
            "election_date": date(2019, 5, 25),
            "article_names": ["Élections_européennes_de_2019_en_France", "Élections_européennes_de_2019"],
        },
    },
    "HU": {
        "language": "hungarian",
        "wikicode": "hu.wikipedia",
        2009: {"election_date": date(2009, 6, 7), "article_names": ["2009-es_európai_parlamenti_választás"]},
        2014: {"election_date": date(2014, 5, 25), "article_names": ["2014-es_magyarországi_országgyűlési_választás"]},
        2019: {"election_date": date(2019, 5, 26), "article_names": ["2019-es_európai_parlamenti_választás"]},
    },
    "IT": {
        "language": "italian",
        "wikicode": "it.wikipedia",
        2009: {
            "election_date": date(2009, 6, 6),
            "article_names": ["Elezioni_europee_del_2019_in_Italia", "Elezioni_europee_del_2009"],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": ["Elezioni_europee_del_2019_in_Italia", "Elezioni_europee_del_2014"],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": ["Elezioni_europee_del_2019_in_Italia", "Elezioni_europee_del_2019"],
        },
    },
    "NL": {
        "language": "dutch",
        "wikicode": "nl.wikipedia",
        2009: {"election_date": date(2009, 6, 4), "article_names": ["Europese_Parlementsverkiezingen_2009"]},
        2014: {"election_date": date(2014, 5, 22), "article_names": ["Europese_Parlementsverkiezingen_2014"]},
        2019: {"election_date": date(2019, 5, 23), "article_names": ["Europese_Parlementsverkiezingen_2019"]},
    },
    "PL": {
        "language": "polish",
        "wikicode": "pl.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": [
                "Wybory_do_Parlamentu_Europejskiego_w_Polsce_w_2009_roku",
                "Wybory_do_Parlamentu_Europejskiego_w_2009_roku",
            ],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": [
                "Wybory_do_Parlamentu_Europejskiego_w_Polsce_w_2014_roku",
                "Wybory_do_Parlamentu_Europejskiego_w_2014_roku",
            ],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": [
                "Wybory_do_Parlamentu_Europejskiego_w_Polsce_w_2019_roku",
                "Wybory_do_Parlamentu_Europejskiego_w_2019_roku",
            ],
        },
    },
    "SE": {
        "language": "swedish",
        "wikicode": "sv.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": ["Europaparlamentsvalet_i_Sverige_2009", "Europaparlamentsvalet_2009"],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": ["Europaparlamentsvalet_i_Sverige_2014", "Europaparlamentsvalet_2014"],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": ["Europaparlamentsvalet_i_Sverige_2019", "Europaparlamentsvalet_2019"],
        },
    },
    "ES": {
        "language": "spanish",
        "wikicode": "es.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": [
                "Elecciones_al_Parlamento_Europeo_de_2009_(Espa%C3%B1a)",
                "Elecciones_al_Parlamento_Europeo_de_2009",
            ],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": [
                "Elecciones_al_Parlamento_Europeo_de_2014_(Espa%C3%B1a)",
                "Elecciones_al_Parlamento_Europeo_de_2014",
            ],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": [
                "Elecciones_al_Parlamento_Europeo_de_2019_(Espa%C3%B1a)",
                "Elecciones_al_Parlamento_Europeo_de_2019_(E]spaña)",
            ],
        },
    },
    "SL": {
        "language": "slovenian",
        "wikicode": "sl.wikipedia",
        2009: {
            "election_date": date(2009, 6, 7),
            "article_names": [
                "Volitve_v_Evropski_parlament_2009",
                "Volitve_poslancev_iz_Slovenije_v_Evropski_parlament_2009",
            ],
        },
        2014: {
            "election_date": date(2014, 5, 25),
            "article_names": [
                "Volitve_v_Evropski_parlament_2014",
                "Volitve_poslancev_iz_Slovenije_v_Evropski_parlament_2014",
            ],
        },
        2019: {
            "election_date": date(2019, 5, 26),
            "article_names": [
                "Volitve_v_Evropski_parlament_2019",
                "Volitve_poslancev_iz_Slovenije_v_Evropski_parlament_2019",
            ],
        },
    },
}


hour_mapping = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
    "E": 4,
    "F": 5,
    "G": 6,
    "H": 7,
    "I": 8,
    "J": 9,
    "K": 10,
    "L": 11,
    "M": 12,
    "N": 13,
    "O": 14,
    "P": 15,
    "Q": 16,
    "R": 17,
    "S": 18,
    "T": 19,
    "U": 20,
    "V": 21,
    "W": 22,
    "X": 23,
}


wikicode_translations = {
    "en": "English",
    "cs": "Czech",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "fi": "Finnish",
    "fr": "French",
    "hu": "Hungarian",
    "it": "Italian",
    "nl": "Dutch",
    "pl": "Polish",
    "se": "Swedish",
    "es": "Spanish",
    "sv": "Swedish",
    "sl": "Slovenian",
}
