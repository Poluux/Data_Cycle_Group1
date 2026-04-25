import sys
import os
import argparse
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(base_dir, 'src'))

from ingest_tickers import ingest_stocks_master, ingest_price_history, portfolio, bronze_path
from process_data import process_stocks_master, process_price_history
from gold import load_dim_date, load_dim_ticker, load_fact_yfinance, load_fact_technical_indicators
from knime_process_data import knime_send_data_toAPI, send_to_sqlDB
from convert_db_to_csv import export_sql_to_csv

@task(name="Bronze - Stocks Master", retries=2, retry_delay_seconds=30)
def task_ingest_stocks_master():
    ingest_stocks_master(portfolio, bronze_path)

@task(name="Bronze - Price History", retries=2, retry_delay_seconds=30)
def task_ingest_price_history(period):
    ingest_price_history(portfolio, bronze_path, period)

@task(name="Silver - Stocks Master", retries=1, retry_delay_seconds=30)
def task_process_stocks_master():
    process_stocks_master()

@task(name="Silver - Price History", retries=1, retry_delay_seconds=30)
def task_process_price_history():
    process_price_history()
    
@task(name="Gold - DimDate & DimTicker", retries=1, retry_delay_seconds=30)
def task_gold_dims(include_stocks_master: bool):
    load_dim_date()
    if include_stocks_master:
        load_dim_ticker()

@task(name="Gold - Fact Tables", retries=1, retry_delay_seconds=30)
def task_gold_facts():
    # Methods check themselves the last inserted date and add the new rows automatically
    load_fact_yfinance()
    load_fact_technical_indicators()
    
@task(name="Knime - Send data to Knime API", retries=1, retry_delay_seconds=30)
def task_knime_sendData_ToAPI():
    knime_send_data_toAPI()
    
@task(name="Knime - Send data to SQL DB", retries=1, retry_delay_seconds=30)
def task_knime_send_to_DB():
    send_to_sqlDB()

@task(name="SAC - Convert Gold DB to csv", retries=1, retry_delay_seconds=30)
def task_sac_dataConversion():
    export_sql_to_csv()

@flow(name="Medallion Pipeline", log_prints=True)
def pipeline(period: str = "1d", include_stocks_master: bool = False):

    full_run = period != "1d"

    # Bronze
    if include_stocks_master:
        task_ingest_stocks_master()
    task_ingest_price_history(period)

    # Silver - launches automatically after Bronze
    if include_stocks_master:
        task_process_stocks_master()
    task_process_price_history()
    
    task_knime_sendData_ToAPI()
    
    # Gold - launches automatically after Silver
    task_gold_dims(include_stocks_master)
    task_gold_facts()
    
    task_knime_send_to_DB()

    # SAC - data conversion
    task_sac_dataConversion()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="1d")
    parser.add_argument("--stocks-master", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--cron", default="0 22 * * 1-5")
    parser.add_argument("--timezone", default="America/Los_Angeles")
    args = parser.parse_args()

    if args.serve:
        pipeline.serve(
            name="daily-medallion",
            schedules=[CronSchedule(cron=args.cron, timezone=args.timezone)]
        )
    else:
        pipeline(period=args.period, include_stocks_master=args.stocks_master)