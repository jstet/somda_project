from somda_project.sql_queries import extract_page
import importlib.resources as importlib_resources


def test_extract_page():
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("20090521-140000.parquet")

    input_filepath = str(resource_path)

    result = extract_page(input_filepath, "en", "Lake_of_Constance")
    expected_result = ("en", "Lake_of_Constance", "1")
    assert result == expected_result

    # Test for year 2009
    result = extract_page(input_filepath, "cs", "Volby_do_Evropského_parlamentu_2009")
    expected_result = ("cs", "Volby_do_Evropského_parlamentu_2009", "6")
    assert result == expected_result

    # Test for year 2014
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("20140531.parquet")
    input_filepath = str(resource_path)
    result = extract_page(input_filepath, "cs.wikipedia", "Volby_do_Evropského_parlamentu_2014")
    expected_result = ("cs.wikipedia", "Volby_do_Evropského_parlamentu_2014", "B1C1F3H4I2J8K1L7M9N10O4P6Q1R4S2T4U3W1")
    assert result == expected_result

    # Test for year 2019
    resource_path = importlib_resources.files("tests.data.pageviews_parquet").joinpath("20190526.parquet")
    input_filepath = str(resource_path)
    result = extract_page(input_filepath, "cs.wikipedia", "Volby_do_Evropského_parlamentu_2019")
    expected_result = ("cs.wikipedia", "Volby_do_Evropského_parlamentu_2019", "I1L1S1V4W1")
    assert result == expected_result
