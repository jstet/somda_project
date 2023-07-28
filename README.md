## Replication Study of:
Yasseri, T., Bright, J. Wikipedia traffic data and electoral prediction: towards theoretically informed models. EPJ Data Sci. 5, 22 (2016). https://doi.org/10.1140/epjds/s13688-016-0083-3

Final project of the course "[Social Media Data Analysis](https://github.com/dgarcia-eu/SocialMediaDataAnalysis)" at the University of Konstanz.

## Data Retrieval and Processing
This project uses data from two sources: Wikipedia Page View Statistics and European Election Results. Based on this, the data retrieval process is separated into two files: `retrieve_election_results.py` and `retrieve_wiki_stats.py`. The latter is separated further into consecutive sub-pipelines, running remotely on [modal](https://modal.com/)(30$ free credits). Interim results for both data retrieval processes are stored on a remote S3 server (Object storage, 75GB free). 

### Wikipedia Page View Statistics
Wikimedia provides hourly page view statistics of the whole wikipedia (and other projects) on [this](https://dumps.wikimedia.org/other/pageview_complete/) website in the form of large compressed text files. I couldnt use the Wikimedia API because it doesnt include data older than 2015, so I downloaded dumps for the relevant days. Like Yasseri et al. (p. 5), I applied a time range of one week before the election, but included another week afterwards in case I wanted to do further analysis. 

The data retrieval process starts with the generation of a list of URLs to the respective files. Then, making use of modals parallel processing features, all files are downloaded and processed chunkwise at roughly the same time. Processing includes filtering the downloaded text file to only include statistics for articles in the wikipedia project after it was uncompressed. Furthermore, filtered chunks are converted to parquet. The resulting parquet files are finally stored on the S3 server. The next step is to extract page view statistics for the relevant wikipedia articles. Yasseri et al. included statistics of 14 election articles in their corresponding language (p. 4). The languages can be extracted from figure 1. I manually entered the languages into a python ditionary and added the election pages (see data.py). 

### Data Sources:
- Pageview complete dumps. (2020, November 24). Retrieved from https://dumps.wikimedia.org/other/pageview_complete/readme.html
- Turnout data: https://www.europarl.europa.eu/election-results-2019/en/tools/download-datasheets/


