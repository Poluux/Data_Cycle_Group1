import datetime
import sys
import pytz
import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from encryption import decrypt_table, encrypt_table


config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)
    
bronze_dir = Path(base_dir) / config['paths']['bronze']
silver_dir = Path(base_dir) / config['paths']['silver']
date_col = config['columns']['date']
ticker_col = config['columns']['ticker']
close_col = config['columns']['target_close']
numeric_cols = config['columns']['numeric_fields']
encrypted_columns = config['encryption']['encrypted_columns']

def process_stocks_master():
    print("Process latest stocks_master data")
    current_month = datetime.datetime.now().strftime('%Y-%m')
    bronze_path = bronze_dir/'equity_funds' / current_month
    silver_path = silver_dir/'equity_funds'
    silver_path.mkdir(parents=True, exist_ok=True)
    
    files = sorted(list(bronze_path.glob('stocks_master_*.csv')))
    if not files:
        print("- stocks_master: no files found in Bronze to process")
        return
    
    # Sort alphabetically to get the latest file, because it has the date in the name.
    latest_file = files[-1] 
    date_part = latest_file.stem.split('_')[-1]
    print(f"Reading {latest_file.name}")
    
    df = pd.read_csv(latest_file)
    
    df = decrypt_table(df, encrypted_columns)
    
    # Null handling, drop duplicates, string formatting, date to datetime, etc.
    df = df.dropna(subset=[ticker_col])
    df[ticker_col] = df[ticker_col].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=[ticker_col], keep='last')
    
    df['company_name'] = df['company_name'].str.strip().fillna(df[ticker_col])
    df['sector'] = df['sector'].str.strip().fillna('Not specified')
    df['industry'] = df['industry'].str.strip().fillna('Unknown')
    df['currency'] = df['currency'].fillna('Unknown')
    df['exchange'] = df['exchange'].fillna('Unknown')
    df['ingestion_date'] = pd.to_datetime(df['ingestion_date'], format='%Y-%m-%d %H-%M-%S', errors='coerce')
    
    # Save to Silver with the same date in the name. Use parquet for better performance.
    file_name = f"clean_stocks_master_{date_part}.parquet"
    save_path = silver_path / file_name
    
    df.to_parquet(save_path.resolve(), index=False)
    
    print(f"- stocks_master: processed data saved in {silver_path}")
    
def process_price_history():
    print("Process price_history data of each ticker")
    silver_path = silver_dir/'price_history'
    silver_path.mkdir(parents=True, exist_ok=True)
    
    silver_files = list(silver_path.glob('clean_price_history_*.parquet'))
    ny_tz = pytz.timezone('US/Eastern')
    today_date = datetime.datetime.now(ny_tz).strftime('%Y-%m-%d')
    
    # Process all data if Silver is empty, if else, only process today's data. 
    if len(silver_files) == 0:
        print("- price_history: Silver is empty. Running full load...")
        files = list((bronze_dir / 'price_history').rglob('*.csv'))
    else:
        files = list((bronze_dir / 'price_history').rglob(f'*_{today_date}.csv'))
    
    if not files:
        print(f"- price_history: no files found in Bronze to process.")
        return
    
    # Concatenate the info of each ticker to a single dataframe
    print(f'Processing {len(files)} price_history files...')
    all_dfs = []
    for file in files:
        df_temp = pd.read_csv(file)
        all_dfs.append(df_temp)
    df = pd.concat(all_dfs, ignore_index=True)
    
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    df = decrypt_table(df, encrypted_columns)
    
    #  Drop duplicates, sort, date to datetime, numerical fields to numeric, string formatting, delete rows with null in 'close'...
    
    if date_col in df.columns:
        temp_date = pd.to_datetime(df[date_col], utc=True)
        
        df[date_col] = temp_date.dt.date

    if ticker_col in df.columns:
        df[ticker_col] = df[ticker_col].astype(str).str.upper().str.strip()
        
    cols_to_convert = [c for c in numeric_cols if c in df.columns]
    for col in cols_to_convert:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'dividends' in df.columns:
        df['has_dividend'] = df['dividends'] > 0
    
    if 'stock_splits' in df.columns:
        df['has_split'] = df['stock_splits'] > 0
    
    # Additional metrics: intraday volatility, session change, session change percentage
    if set(['high', 'low', 'open', 'close']).issubset(df.columns):
        df['intraday_volatility'] = df['high'] - df['low']
        df['session_change'] = df['close'] - df['open']
        df['session_change_pct'] = np.where(df['open'] != 0, (df['session_change'] / df['open']) * 100, 0.0)
    
    if close_col in df.columns:
        df = df.dropna(subset=[close_col])
        
    if date_col in df.columns and ticker_col in df.columns:    
        df = df.drop_duplicates(subset=[ticker_col, date_col], keep='last').copy()
        df = df.sort_values(by=[ticker_col, date_col], ascending=[True, True])
    else:
        print(f"Warning: columns {date_col} or {ticker_col} not found")
    
    # Save data for each ticker. Use parquet for better performance.
    processed_tickers = df[ticker_col].unique()
    
    for ticker in processed_tickers:
        df_new_data = df[df[ticker_col] == ticker]
        
        file_name = f"clean_price_history_{ticker}.parquet"
        save_path = silver_path / file_name
        
        initial_rows = 0
        
        # If the file already exists, load it to combine with the new data
        if save_path.exists():
            df_old_history = pd.read_parquet(save_path)
            df_old_history = decrypt_table(df_old_history, encrypted_columns)
            initial_rows = len(df_old_history)
            df_combined = pd.concat([df_old_history, df_new_data], ignore_index=True)
            
            # Delete duplicates if there are any today
            df_combined = df_combined.drop_duplicates(subset=[ticker_col, date_col], keep='last')
            df_combined = df_combined.sort_values(by=[ticker_col, date_col])
        else:
            df_combined = df_new_data
        
        added_rows = len(df_combined) - initial_rows
        
        if added_rows > 0:
            df_combined = encrypt_table(df_combined.copy(), encrypted_columns)
            df_combined.to_parquet(save_path, index=False)
            print(f"- {ticker}: {added_rows} new rows added. (Total: {len(df_combined)})")
        else:
            print(f"- {ticker}: no new data found, skipping Silver update.")
        
    print(f"- price_history: Total data processed: {len(df)} rows. Process completed.")
    
if __name__ == "__main__":  
    include_stocks_master = "--stocks-master" in sys.argv
    
    if include_stocks_master:
        process_stocks_master()
        
    process_price_history()
