import requests
import calendar


def wiki_page_views_old():
    base_url = "https://dumps.wikimedia.org/other/pageview_complete/"

    start_year = 2012
    end_year = 2015

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > 6:
                break
            num_days = calendar.monthrange(year, month)[1]
            for day in range(1, num_days + 1):
                url = f"{base_url}{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2"
                print(url)
                response = requests.get(url)
                if response.status_code == 200:
                    file_path = f"pageviews-{year}{month:02d}{day:02d}-user.bz2"
                    with open(file_path, "wb") as file:
                        file.write(response.content)
                    print(f"File saved: {file_path}")
                else:
                    print(f"No data found for {year}-{month:02d}-{day:02d}")


def wiki_page_views_new():
    pass


def german_election_resulsts():
    pass
