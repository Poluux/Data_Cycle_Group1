import sys
import os
import json
import pandas as pd
import time
import datetime
import pytz
import subprocess
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from pathlib import Path
from encryption import decrypt_table, encrypt_table
from db_connection import get_engine

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)

silver_dir = Path(base_dir) / config['paths']['silver']
knime_silver_process_dir = Path(base_dir) / config['paths']['knime_silver_process']
output_dir = Path(base_dir) / config['paths']['knime_silver_process'] / "predictions"

# Create folder for knime
knime_silver_process_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)

# Load depending on context of execution
if os.getenv("RUNNING_IN_DOCKER"):
    load_dotenv(".env",override=True)
else:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"),override=True)

engine = get_engine()

def get_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Missing env var: {name}")
    return value


knime_api_ID = get_env("KNIME_API_ID")
knime_api_PWD = get_env("KNIME_API_PASSWORD")

KNIME_APIS = {
    "AMZN": config["paths"]["knime_api_url_AMZN"],
    "MSFT": config["paths"]["knime_api_url_MSFT"],
    "NVDA": config["paths"]["knime_api_url_NVDA"]
}
    
def knime_send_data_toAPI():
    print("Knime, process predictions for tickers")
    
    los_angeles_tz = pytz.timezone('America/Los_Angeles')
    
    knime_prediction_output_path = knime_silver_process_dir / 'predictions'
    knime_prediction_output_path.mkdir(parents=True, exist_ok=True)
    
    silver_path = silver_dir / 'price_history'
    
    files = list(silver_path.glob("clean_price_history_*.parquet"))
    
    if not files:
        print("No parquet files found")
        return
    
    for file in files:
        print(f"\nProcessing {file.name}")
        
        df = pd.read_parquet(file)
        df = decrypt_table(df)
        
        ticker= file.stem.replace("clean_price_history_","")
        
        if ticker not in KNIME_APIS:
            print(f"Unknown ticker: {ticker}, skipping")
            continue
        
        try:
            response_json = send_to_knime(df, KNIME_APIS[ticker])
            
            print(f"KNIME done for {ticker}")
            
            df_out = parse_knime_output(response_json)
            
            print(f"Output rows: {len(df_out)}")
            
            save_encrypted(df_out, ticker)
            
        except Exception as e:
            print(f"Error for {ticker}: {e}")
    
    print("KNIME workflow finished")
    
def build_payload(df: pd.DataFrame):

    df = df.astype(str)

    table_spec = [{col: "string"} for col in df.columns]
    table_data = df.values.tolist()

    return {
        "table-input": {
            "table-spec": table_spec,
            "table-data": table_data
        }
    }
    
def send_to_knime(df: pd.DataFrame, api_url: str):

    payload = build_payload(df)

    response = requests.post(
        api_url,
        json=payload,
        auth=HTTPBasicAuth(knime_api_ID, knime_api_PWD)
    )

    response.raise_for_status()
    return response.json()

def parse_knime_output(json_response: dict) -> pd.DataFrame:
    table = json_response["outputValues"]["table-output"]

    columns = [list(col.keys())[0] for col in table["table-spec"]]
    data = table["table-data"]

    rows_as_dicts = [
        dict(zip(columns, row))
        for row in data
    ]

    df = pd.DataFrame(rows_as_dicts)
    
    # add today date based on Los Angeles
    los_angeles_tz = pytz.timezone('America/Los_Angeles')
    today_la = datetime.datetime.now(los_angeles_tz).date()
    df['date_of_processing'] = today_la

    return df

def save_encrypted(df: pd.DataFrame, ticker: str):
    df_encrypted = encrypt_table(df)

    file_path = output_dir / f"encrypted_predictions_{ticker}.parquet"

    df_encrypted.to_parquet(file_path, index=False)

    print(f"Saved encrypted predictions: {file_path}")
    
def get_ticker_id_map():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, ticker FROM DimTicker"))
        return {row.ticker: row.id for row in result}
    
def get_date_id_map():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, date FROM dimDate"))
        return {pd.to_datetime(row.date).date(): row.id for row in result}
    
def send_to_sqlDB():
    print("Sending Predictions to SQL Database")
    
    files = list(output_dir.glob("encrypted_predictions_*.parquet"))
    if not files:
        print("- Send_to_DB: no files found in predictions folder")
        return
    
    ticker_map = get_ticker_id_map()
    if not ticker_map:
        print("- Send_to_DB: DimTicker is emtpy, run with --stocks-master first")
        return
    
    date_map = get_date_id_map()
    
    for file in files:
        df = pd.read_parquet(file)
        df = decrypt_table(df)
        
        ingestion_date = pd.to_datetime(df['date_of_processing'].iloc[0]).date()
        ticker = df['Ticker-Of-Iteration'].iloc[0]
        
        if ticker not in ticker_map:
            print(f"- {ticker}: not found in DimTicker, skipping")
            continue
        
        ticker_id = ticker_map[ticker]
        ingestion_date_id = date_map[ingestion_date]
        
        df['date'] = pd.to_datetime(df['date']).dt.date.map(date_map)
        df['Prediction (close)'] = pd.to_numeric(df['Prediction (close)'], errors='coerce')
        df['Ticker-Of-Iteration'] = ticker_id
        df['date_of_processing'] = ingestion_date_id
        
        df = df.rename(columns={
            'date': 'Date_FK',
            'Prediction (close)': 'PredictedPrice',
            'Ticker-Of-Iteration': 'Ticker_FK',
            'date_of_processing': 'Ingestion_Date_FK',
        })
        
        try:
            df.to_sql('Fact_Prediction', con=engine, if_exists='append', index=False)
            print(f"- Send_to_DB: {ticker}: {len(df)} new rows inserted into Fact_Prediction")
        except Exception as e:
            if '2627' in str(e) or 'PRIMARY KEY' in str(e):
                print(f"- {ticker}: already up to date, skipping")
            else:
                raise
    
    
        
if __name__ == "__main__":    
   knime_send_data_toAPI()
   send_to_sqlDB()