# Predicting Election Outcomes using Wikipedia Page Views

## Replication Study of:
Yasseri, T., Bright, J. Wikipedia traffic data and electoral prediction: towards theoretically informed models. EPJ Data Sci. 5, 22 (2016). https://doi.org/10.1140/epjds/s13688-016-0083-3

Final project of the course "[Social Media Data Analysis](https://github.com/dgarcia-eu/SocialMediaDataAnalysis)" at the University of Konstanz.

Find the final project report at `report/main.pdf`

## Project Structure
This python project is managed using poetry. It mainly consists of 
- a python module `somda_project` containing functions utilized in data retrieval and analysis
- two jupyter notebooks for data analysis (`analysis.ipynb`) and exploration (`exploration.ipynb`) 
- two python files that orchestrate the retrieval and processing for the two data sources of this project, namely `retrieve_election_results.py` and `retrieve_wiki_stats.py`
- the folder `data` containing retrieved data used for analysis
- the folder `report` containing LaTeX code to render the final report

## Dev Setup
1. Install Requirements (also requirements in the dev group) using [Poetry](https://python-poetry.org/)
2. Initialize [Modal](https://modal.com/)

### Data Retrieval
Having installed the requirements and initialized modal, you can run the retrieval_*.py files. 

### Data Analysis
Start up your Jupyter server and open the corresponding notebooks.

## Quality Ensurance
- Pre-Commit is set up to enforce linting with ruff and formatting with black
- pytest is used for unit testing, run `poetry run pytest` to start testing





