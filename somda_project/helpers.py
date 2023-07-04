import calendar
from somda_project.console import console
import os
import requests
import time


def gen_urls_old_wiki():
    base_url = "https://dumps.wikimedia.org/other/pageview_complete/"

    start_year = 2013
    end_year = 2015
    urls = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > 6:
                break
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                url = f"{base_url}{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2"
                urls.append({"url": url, "id": f"{year}{month:02d}{day:02d}"})

    return urls


def download_file(url):
    console.log(f"Starting download for {url['id']}")
    with requests.get(url["url"], stream=True) as raw:
        total_length = int(raw.headers.get("Content-Length"))
        filepath = f"temp_{os.path.basename(url['id'])}.bz2"
        with open(filepath, "wb") as output:
            start_time = time.time()
            update_time = start_time + 10
            for chunk in raw:
                output.write(chunk)
                if time.time() >= update_time:
                    elapsed_time = time.time() - start_time
                    downloaded = output.tell()
                    speed = downloaded / elapsed_time
                    progress = min(downloaded / total_length, 1.0) * 100
                    console.log(f"Progress: {progress:.2f}%, Speed: {speed:.2f} B/s")
                    update_time += 10
    return filepath
