import yfinance as yf
import pandas as pd
import os
import datetime
from encryption import encrypt_table
import sys

portfolio = ['AMZN', 'MSFT', 'NVDA']
bronze_path = './data/bronze'

def ingest_stocks_master(portfolio, bronze_path):
    path = f'{bronze_path}/equity_funds'
    os.makedirs(path, exist_ok=True)

    print("Download tickers from Yahoo Finance")
    results = []
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            info = dat.info

            if not info or (info.get('longName') is None and info.get('shortName') is None):
                print(f"- {ticker} doesn't exist or doesn't have data")
                continue
            
            data = {
                'ticker': ticker,
                'company_name': info.get('longName') or info.get('shortName'),
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'currency': info.get('currency'),
                'exchange': info.get('exchange'),
                'ingestion_date': datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            }
            results.append(data)
            print(f"- {ticker} processed")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
        
    if results:
        df = pd.DataFrame(results)
        today_date = datetime.datetime.now().strftime('%Y-%m-%d')
        file_name = f"stocks_master_{today_date}.csv"
        
        df = encrypt_table(df) # Encryption of the table
        df.to_csv(f"{path}/{file_name}", index=False)
        print(f"Tickers saved in {path}/{file_name}")
    else:
        print("No tickers to save")
        
def ingest_price_history(portfolio, bronze_path, period):
    path = f'{bronze_path}/price_history'
    os.makedirs(path, exist_ok=True)

    print(f"Download price history from Yahoo Finance (period: {period})")
    for ticker in portfolio:
        try:
            # Check if file already exists for today
            today_date = datetime.datetime.now().strftime('%Y-%m-%d')
            file_name = f"price_history_{ticker}_{today_date}.csv"
            
            if os.path.exists(f"{path}/{file_name}"):
                print(f"- {ticker} already ingested today, skipping")
                continue

            dat = yf.Ticker(ticker)
            df = dat.history(period=period, auto_adjust=False)
            
            if df.empty:
                print(f"- {ticker} doesn't have price history data")
                continue
            
            df = df.reset_index()
            df["Ticker"] = ticker
            df["ingestion_date"] = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
          
            # Save to Bronze layer
            today_date = datetime.datetime.now().strftime('%Y-%m-%d')
            file_name = f"price_history_{ticker}_{today_date}.csv"
            
            df = encrypt_table(df) # Encryption
            df.to_csv(f"{path}/{file_name}", index=False)
            print(f"- {ticker} price history saved in {path}/{file_name}")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
        
if __name__ == "__main__":
    period = sys.argv[1] if len(sys.argv) > 1 else "1d"
    include_stocks_master = "--stocks-master" in sys.argv

    if include_stocks_master:
        ingest_stocks_master(portfolio, bronze_path)
        
    ingest_price_history(portfolio, bronze_path, period)