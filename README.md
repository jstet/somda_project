## Replication Study of:
Yasseri, T., Bright, J. Wikipedia traffic data and electoral prediction: towards theoretically informed models. EPJ Data Sci. 5, 22 (2016). https://doi.org/10.1140/epjds/s13688-016-0083-3

Final project of the course "[Social Media Data Analysis](https://github.com/dgarcia-eu/SocialMediaDataAnalysis)" at the University of Konstanz.

## Data Retrieval and Processing
This project involves retrieving and processing data from two sources: Wikipedia Page View Statistics and European Election Results. The data retrieval process is divided into two files: retrieve_election_results.py and retrieve_wiki_stats.py. The retrieve_wiki_stats.py file further consists of consecutive sub-pipelines that run remotely on modal (which offers $30 free credits). The interim results for both data retrieval processes are stored on a remote S3 server (Scaleway provides 75GB of free object storage).

#### Wikipedia Page View Statistics
Wikimedia provides hourly page view statistics for Wikipedia and other projects on [their website](https://dumps.wikimedia.org/other/pageview_complete/readme.html). Since the Wikimedia API doesn't include data older than 2015, I downloaded dumps for the relevant days. Following the approach of Yasseri et al. (p. 5), I considered a time range of one week before the election and one week after for potential further analysis.

The data retrieval process starts by generating a list of URLs for the relevant files. Using modal's parallel processing capabilities, all the files are downloaded and processed simultaneously in chunks. The processing involves filtering the downloaded text files to include only statistics for articles within the Wikipedia project. The filtered chunks are then converted to the parquet format. The resulting parquet files are stored on the S3 server.

The next step is to extract page view statistics for the relevant Wikipedia articles. Yasseri et al. included statistics for 14 election articles in their corresponding language. I manually created a Python dictionary with the languages extracted from Figure 1 and added the names of the election page articles (see data.py). This dictionary is used to extract the page view statistics by loading the respective parquet files into an in-memory duckdb database. A SQL query is then executed to select rows based on the article name and the Wikipedia project's language version. The article with the highest number of page views is chosen.

The resulting rows contain information for one day, with hourly page views encoded in a string format like "A38B25C18". To facilitate further analysis, the results are converted into a pandas dataframe. The rows are then exploded, so that each row represents one hour. The dataframe is saved as a CSV file and uploaded. Finally, the individual CSV files are concatenated into one file that contains page view statistics for all election pages during the relevant hours. This consolidated file is saved locally.

### Data Sources:
- Pageview complete dumps. (2020, November 24). Retrieved from https://dumps.wikimedia.org/other/pageview_complete/readme.html
- Turnout data: https://www.europarl.europa.eu/election-results-2019/en/tools/download-datasheets/


