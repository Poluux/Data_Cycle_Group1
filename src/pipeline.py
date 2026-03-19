import sys
import os
import argparse
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(base_dir, 'src'))

from ingest_tickers import ingest_stocks_master, ingest_price_history, portfolio, bronze_path
from process_data import process_stocks_master, process_price_history

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

@flow(name="Medallion Pipeline", log_prints=True)
def pipeline(period: str = "1d", include_stocks_master: bool = False):

    # Bronze
    if include_stocks_master:
        task_ingest_stocks_master()
    task_ingest_price_history(period)

    # Silver — se lance après Bronze automatiquement
    if include_stocks_master:
        task_process_stocks_master()
    task_process_price_history()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="1d")
    parser.add_argument("--stocks-master", action="store_true")
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()

    if args.serve:
        pipeline.serve(
            name="daily-medallion",
            schedules=[CronSchedule(cron="* 22 * * 1-5", timezone="America/New_York")]
        )
    else:
        pipeline(period=args.period, include_stocks_master=args.stocks_master)