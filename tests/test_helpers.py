from somda_project.helpers import download_file
import os


def test_download_file():
    filepath = download_file(
        {
            "url": "https://dumps.wikimedia.org/other/pageview_complete/2013/2013-01/pageviews-20130101-user.bz2",
            "id": "20130101",
        }
    )
    assert os.path.isfile(filepath)
    os.remove(filepath)
