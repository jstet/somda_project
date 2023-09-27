from somda_project.helpers import gen_urls
import requests


def test_gen_urls():
    urls = gen_urls()
    for url in urls:
        print(url)
        response = requests.head(url["url"])
        assert response.status_code == 200
