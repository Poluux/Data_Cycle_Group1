import yfinance as yf
import pandas as pd
import os
import datetime

# Tickers list (Added examples of equities, funds, tickers from other sectors, fake tickers...)
portfolio = ['AMZN', 'MSFT', 'NVDA']
bronze_path = './data/bronze'

def ingest_stocks_master(portfolio, bronze_path):
    # Directory to save files
    path = f'{bronze_path}/equity_funds'
    os.makedirs(path, exist_ok=True)

    print("Download tickers from Yahoo Finance")
    results = []
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            
            info = dat.info

            # Validate that the ticker exists or has data
            if not info or (info.get('longName') is None and info.get('shortName') is None):
                print(f"- {ticker} doesn't exist or doesn't have data")
                continue
            
            # Extract fields for the table 'stocks_master'
            data = {
                'ticker': ticker,
                
                # With null handling
                'company_name': info.get('longName') or info.get('shortName') or ticker, 
                'sector': info.get('sector') or 'Sector not specified',
                'industry': info.get('industry') or 'Industry not specified',
                'currency': info.get('currency') or 'Currency unknown',
                'exchange': info.get('exchange') or 'Exchange unknown',
                'ingestion_date': datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
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
        
        df.to_csv(f"{path}/{file_name}", index=False)
        print(f"Tickers saved in {path}/{file_name}")
    else:
        print("No tickers to save")
        
def ingest_price_history(portfolio, bronze_path):
    # Directory to save files
    path = f'{bronze_path}/price_history'
    os.makedirs(path, exist_ok=True)

    print("Download price history from Yahoo Finance")
    for ticker in portfolio:
        try:
            dat = yf.Ticker(ticker)
            df = dat.history(period="10y", auto_adjust=False)
            
            if (df.empty):
                print(f"- {ticker} doesn't have price history data")
                continue
            
            df = df.reset_index()
            
            df["Ticker"] = ticker
            df = df.rename(columns={
                "Date": "calendar",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Adj Close": "adj_close"
            })

            df = df.drop(columns=["Dividends", "Stock Splits"], errors="ignore")
            
            # Save to Bronze layer
            today_date = datetime.datetime.now().strftime('%Y-%m-%d')
            file_name = f"price_history_{ticker}_{today_date}.csv"
            
            df.to_csv(f"{path}/{file_name}", index=False)
            print(f"- {ticker} price history saved in {path}/{file_name}")
                
        except Exception as e:
            print(f"- {ticker} error: {e}")
    
    
        
if __name__ == "__main__":
    ingest_stocks_master(portfolio, bronze_path)
    ingest_price_history(portfolio, bronze_path)