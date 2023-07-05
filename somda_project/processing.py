import duckdb


def process_parquet(input_filepath):
    temp = duckdb.query(f"""
    SELECT *
    FROM '{input_filepath}'
    WHERE article_title LIKE '%Alternative%' AND wikicode = 'de.wikipedia'
    """).fetchall()

    print(temp)
