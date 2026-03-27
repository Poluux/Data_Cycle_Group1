import json
import sys
import time
import uuid
import pytz
import yfinance as yf
import pandas as pd
import os
import datetime
from pathlib import Path
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from encryption import encrypt_table

config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)
    
portfolio = config['portfolio']
bronze_path = Path(base_dir) / config['paths']['bronze']

def ingest_stocks_master(portfolio, bronze_path):
    # Get the current date in New York time
    ny_tz = pytz.timezone('US/Eastern')
    current_month = datetime.datetime.now(ny_tz).strftime('%Y-%m')
    path = Path(bronze_path) / 'equity_funds' / current_month

    os.makedirs(path, exist_ok=True)

    today_date = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d')
    file_name = f"stocks_master_{today_date}.csv"

    if (path / file_name).exists():
        print(f"- stocks_master: already ingested today, skipping Bronze ingestion")
        return

    print("Download tickers from Yahoo Finance")
    results = []
    
    execution_id = str(uuid.uuid4())

    # Precalculate the ingestion timestamp to have the same value for all tickers
    ingestion_timestamp = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d %H-%M-%S')
    
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            info = dat.info
            
            # True raw / Save original data from API
            raw_file_name = f"raw_info_{ticker}_{today_date}.json"
            raw_save_path = path / raw_file_name
            with open(raw_save_path, 'w', encoding='utf-8') as json_file:
                json.dump(info, json_file, ensure_ascii=False, indent=4)

            # Validate that the ticker exists or has data
            if not info or (info.get('longName') is None and info.get('shortName') is None):
                print(f"- {ticker} doesn't exist or doesn't have data")
                continue
            
            # Extract fields for the table 'stocks_master'. Fields renamed correctly.
            data = {
                'ticker': ticker,
                'company_name': info.get('longName') or info.get('shortName'), 
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'currency': info.get('currency'),
                'exchange': info.get('exchange'),
                'ingestion_date': ingestion_timestamp,
                'execution_id': execution_id
            }
            results.append(data)
            print(f"- {ticker} ingested successfully")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
        
    # Save to Bronze layer
    # It creates a different file each day. To not overwrite, the file has the date in the name
    if results:
        df = pd.DataFrame(results)
        df = encrypt_table(df) 
        save_path = path / file_name
        df.to_csv(save_path, index=False)
        print(f"- stocks_master: saved in {save_path}")
    else:
        print("- stocks_master: no tickers to save")
        
def ingest_price_history(portfolio, bronze_path, period):
    ny_tz = pytz.timezone('US/Eastern')
    current_month = datetime.datetime.now(ny_tz).strftime('%Y-%m')
    path = Path(bronze_path) / 'price_history' / current_month
    os.makedirs(path, exist_ok=True)

    print("Download price history from Yahoo Finance")
    today_date = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d')
    ingestion_timestamp = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d %H-%M-%S')

    execution_id = str(uuid.uuid4())
    
    for ticker in portfolio:
        # Indicate if the file has the 10y histical data
        if period in ['max', '10y']:
            file_name = f"price_history_{ticker}_historical_{today_date}.csv"
        else:
            file_name = f"price_history_{ticker}_{today_date}.csv"

        if (path / file_name).exists():
            print(f"- {ticker}: already ingested today, skipping")
            continue

        # Retry logic to handle network issues
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Download price history from Yahoo Finance
                dat = yf.Ticker(ticker)
                df = dat.history(period=period, auto_adjust=False)
            
                if (df.empty):
                    print(f"- {ticker} doesn't have price history data")
                    break
                
                df = df.reset_index()
                df["Ticker"] = ticker
                df["ingestion_date"] = ingestion_timestamp
                df["execution_id"] = execution_id
                
                save_path = path / file_name
                df = encrypt_table(df)
                df.to_csv(save_path, index=False)
                print(f"- {ticker}: price history saved in {save_path}")
                break
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt 
                    print(f"- {ticker} connection error. Retrying in {wait} seconds...")
                    time.sleep(wait)
                else:
                    print(f"- {ticker} critical error after {max_retries} attempts: {e}")
    
if __name__ == "__main__":
    period = sys.argv[1] if len(sys.argv) > 1 else "1d"
    include_stocks_master = "--stocks-master" in sys.argv

    if include_stocks_master:
        ingest_stocks_master(portfolio, bronze_path)
        
    ingest_price_history(portfolio, bronze_path, period)