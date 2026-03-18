import json
import sys

import yfinance as yf
import pandas as pd
import os
import datetime
from pathlib import Path
from encryption import encrypt_table

with open('../config/settings.json') as f:
    config = json.load(f)
    
portfolio = config['portfolio']
bronze_path = Path(config['paths']['bronze'])

def ingest_stocks_master(portfolio, bronze_path):
    # Directory to save files
    path = f'{bronze_path}/equity_funds'
    os.makedirs(path, exist_ok=True)

    print("Download tickers from Yahoo Finance")
    results = []
    
    # Precalculate the ingestion timestamp to have the same value for all tickers
    ingestion_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            info = dat.info

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
                'ingestion_date': ingestion_timestamp
            }
            results.append(data)
            print(f"- {ticker} processed")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
        
    # Save to Bronze layer
    # It creates a different file each day. To not overwrite, the file has the date in the name
    if results:
        df = pd.DataFrame(results)
        today_date = datetime.datetime.now().strftime('%Y-%m-%d')
        file_name = f"stocks_master_{today_date}.csv"
        
        df = encrypt_table(df) 
        save_path = f"{path}/{file_name}"
        df.to_csv(save_path, index=False)
        print(f"Tickers saved in {path}/{file_name}")
    else:
        print("No tickers to save")
        
def ingest_price_history(portfolio, bronze_path, period):
    path = f'{bronze_path}/price_history'
    os.makedirs(path, exist_ok=True)

    print("Download price history from Yahoo Finance")
    
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    ingestion_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            df = dat.history(period=period, auto_adjust=False)
            
            if (df.empty):
                print(f"- {ticker} doesn't have price history data")
                continue
            
            df = df.reset_index()
            df["Ticker"] = ticker
            df["ingestion_date"] = ingestion_timestamp
            
            file_name = f"price_history_{ticker}_{today_date}.csv"
            save_path = f"{path}/{file_name}"
            
            df = encrypt_table(df)
            df.to_csv(save_path, index=False)
            print(f"- {ticker} price history saved in {save_path}")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
    
    
        
if __name__ == "__main__":
    period = sys.argv[1] if len(sys.argv) > 1 else "1d"
    include_stocks_master = "--stocks-master" in sys.argv

    if include_stocks_master:
        ingest_stocks_master(portfolio, bronze_path)
        
    ingest_price_history(portfolio, bronze_path, period)