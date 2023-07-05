from somda_project.helpers import download_file, to_parquet
import importlib_resources
import os


def test_download_file():
    filepath, _id = download_file(
        {
            "url": "https://dumps.wikimedia.org/other/pageview_complete/2013/2013-01/pageviews-20130101-user.bz2",
            "id": "20130101",
        }
    )
    assert os.path.isfile(filepath)
    os.remove(filepath)


def test_to_parquet():
    resource_path = importlib_resources.files("tests.data.pageviews_raw").joinpath("pageviews.bz2")
    path = str(resource_path)  # Convert the Path object to a string
    print(path)
    to_parquet(path, "20130101")
