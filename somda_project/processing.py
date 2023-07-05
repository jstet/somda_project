import duckdb


def process_parquet(input_filepath):
    temp = duckdb.query(f"""
    SELECT COUNT(*)
    FROM '{input_filepath}'
    """).fetchall()

    print(temp)
