# What you will find on this repository
- A dockerised solution that does a full data cycle on data coming from YFinance, from ingestion of data to machine learning and the possibility to do business intelligence
- Knime workflows in the Knime_Workflows folder if you want to modify some things for your cases and then create your API
- PowerBI templates
- Data ingestions are organized around a Medallion architecture (Bronze, Silver, Gold)

# How to install and use our app
The app comes with two ways to be installed and used, one with docker, one without
## Installation
### Dokcer
- First of all install docker on your computer
- Then at the root folder, enter `docker compose up`
- Then you need to create a .env file at the root folder and follow the .env.example
- Then you have two command lines at your disposal. One for a manual ingestion `docker compose --profile init run --rm --build pipeline-init` another one for an automated scheduling `docker compose --profile serve up -d --build pipeline-serve`
### Python Vanilla
- Ensure you have python installed
- Go to the root folder and create a python environment with `python -m venv .venv`
- Then got to the src folder and do `pip install -r requirements.txt`
- Create your database with our init.sql
- In the src folder, create a .env.local following the .env.example
- Launch in a terminal `prefect server start`
- For a first manual ingestion use `py .\src\pipeline.py --period max --stocks-master` for an automated scheduling use `py .\src\pipeline.py --serve`
