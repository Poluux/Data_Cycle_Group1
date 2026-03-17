import yfinance as yf
import pandas as pd
import os
import datetime
import glob

portfolio = ['AMZN', 'MSFT', 'NVDA']
bronze_path = './data/bronze'

def update_price_history(portfolio, bronze_path):
    path = f'{bronze_path}/price_history'
    os.makedirs(path, exist_ok=True)

    print("Incremental update of price history from Yahoo Finance")
    for ticker in portfolio:
        try:
            # Find the latest file for this ticker
            existing_files = glob.glob(f"{path}/price_history_{ticker}_*.csv")
            
            if not existing_files:
                print(f"- {ticker} no existing file found, run init_data.py first")
                continue
            
            # Get the most recent file
            latest_file = max(existing_files)
            df_existing = pd.read_csv(latest_file)
            df_existing["Date"] = pd.to_datetime(df_existing["Date"], utc=True)
            
            last_date = df_existing["Date"].max()
            print(f"- {ticker} last date in file: {last_date.date()}")
            
            # Download from last known date
            df_new = yf.Ticker(ticker).history(
                start=last_date,
                auto_adjust=False
            )
            
            if df_new.empty:
                print(f"- {ticker} no new data available")
                continue
            
            df_new = df_new.reset_index()
            df_new["Ticker"] = ticker
            df_new["ingestion_date"] = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            
            # Align timezones before comparison
            df_new["Date"] = pd.to_datetime(df_new["Date"], utc=True)
            
            # Remove rows already in the existing file (avoid duplicates)
            df_new = df_new[df_new["Date"] > last_date]
            
            if df_new.empty:
                print(f"- {ticker} already up to date")
                continue
            
            # Append to existing file and save with today's date
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
            
            today_date = datetime.datetime.now().strftime('%Y-%m-%d')
            file_name = f"price_history_{ticker}_{today_date}.csv"
            df_updated.to_csv(f"{path}/{file_name}", index=False)
            print(f"- {ticker} updated, {len(df_new)} new rows added → {file_name}")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")

if __name__ == "__main__":
    update_price_history(portfolio, bronze_path)