import sys
import os
import json
import pandas as pd
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

def process_stocks_master():
    print("Process latest stocks_master data")
    bronze_path = bronze_dir/'equity_funds'
    silver_path = silver_dir/'equity_funds'
    silver_path.mkdir(parents=True, exist_ok=True)
    
    files = sorted(list(bronze_path.glob('stocks_master_*.csv')))
    if not files:
        print("Couldn't find any files in Bronze for stocks_master")
        return
    
    # Sort alphabetically to get the latest file, because it has the date in the name.
    latest_file = files[-1] 
    date_part = latest_file.stem.split('_')[-1]
    print(f"Reading {latest_file.name}")
    
    df = pd.read_csv(latest_file)
    
    df = decrypt_table(df)
    
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
    file_name = f"stocks_master_clean_{date_part}.parquet"
    save_path = silver_path / file_name
    
    df.to_parquet(save_path.resolve(), index=False)
    
    print(f"Data processed in {silver_path}/{file_name}")
    
def process_price_history():
    print("Process price_history data of each ticker")
    bronze_path = bronze_dir/'price_history'
    silver_path = silver_dir/'price_history'
    silver_path.mkdir(parents=True, exist_ok=True)
    
    files = list(bronze_path.glob('price_history_*.csv'))
    if not files:
        print("Couldn't find any files in Bronze for price_history")
        return
    
    # Concatenate the info of each ticker to a single dataframe
    print(f'Processing {len(files)} price_history files...')
    all_dfs = []
    for file in files:
        df_temp = pd.read_csv(file)
        all_dfs.append(df_temp)
    df = pd.concat(all_dfs, ignore_index=True)
    
    df = decrypt_table(df)
    
    #  Drop duplicates, sort, date to datetime, numerical fields to numeric, string formatting, delete rows with null in 'close'...
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    if date_col in df.columns:
        temp_date = pd.to_datetime(df[date_col], utc=True)
        
        # Add new date columns
        df['year'] = temp_date.dt.year
        df['month'] = temp_date.dt.month
        df['quarter'] = temp_date.dt.quarter
        df['day_of_week'] = temp_date.dt.day_name()
        
        df[date_col] = temp_date.dt.date

    if ticker_col in df.columns:
        df[ticker_col] = df[ticker_col].astype(str).str.upper().str.strip()
        
    cols_to_convert = numeric_cols
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
    
    # Save all data to a single file. Use parquet for better performance.
    file_name = "price_history_all.parquet"
    save_path = silver_path / file_name
    
    df.to_parquet(save_path, index=False)
    
    print(f"Data processed and saved in {save_path.resolve()} (Total rows: {len(df)})")
    
if __name__ == "__main__":
    process_stocks_master()
    process_price_history()