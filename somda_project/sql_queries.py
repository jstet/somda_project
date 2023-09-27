import duckdb


def extract_page(input_filepath: str, wikicode: str, page_name: str) -> tuple:
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
    WHERE  article_title = '{page_name}' AND wikicode = '{wikicode}'
   
    """).fetchall()
    if temp:
        # return page with highest daily views
        temp = max(temp, key=lambda x: x[2])
        return temp
    else:
        return tuple()
