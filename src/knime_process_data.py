import sys
import os
import json
import pandas as pd
import time
import datetime
import pytz
import subprocess
import requests
from requests.auth import HTTPBasicAuth
from pathlib import Path
from encryption import decrypt_table

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)

silver_dir = Path(base_dir) / config['paths']['silver']    
knime_silver_dir = Path(base_dir) / config['paths']['knime_silver']

# Create folder for knime
knime_silver_dir.mkdir(parents=True, exist_ok=True)

def knime_process_stocks_master():
    print("Knime, process latest stocks_master data")
    silver_path = silver_dir / 'equity_funds'
    knime_silver_path = knime_silver_dir / 'equity_funds'
    knime_silver_path.mkdir(parents=True, exist_ok=True)
    
    files = list(silver_path.glob('clean_stocks_master_*.parquet'))
    if not files:
        print("- Stocks_master: no files found in Silver")
    
    for file in files:
        print(f"Processing: {file.name}")
        
        df = pd.read_parquet(file)
        df = decrypt_table(df)
        
        file_date = file.stem.replace("clean_stocks_master_", "")
        new_file_name = f"uncrypted_knime_stocks_master_{file_date}.parquet"
        save_path = knime_silver_path / new_file_name
        
        df.to_parquet(save_path.resolve(), index=False)
        
        print(f"- Stocks_master: processed decrypted data saved in {save_path}")
        
    
    
def knime_process_price_history():
    print("Knime, process latest price_history data")
    
    silver_path = silver_dir / 'price_history'
    knime_silver_path = knime_silver_dir / 'price_history'
    knime_silver_path.mkdir(parents=True, exist_ok=True)
    
    files = list(silver_path.glob('clean_price_history_*.parquet'))
    if not files:
        print("- Price_history: no files found in Silver price_history")
        return
    
    for file in files:
        print(f"Processing: {file.name}")
        
        df = pd.read_parquet(file)
        df = decrypt_table(df)
        
        ticker = file.stem.replace("clean_price_history_", "")
        new_file_name = f"uncrypted_knime_price_history_{ticker}.parquet"
        save_path = knime_silver_path / new_file_name
        
        df.to_parquet(save_path.resolve(), index=False)
        
        print(f"- Price_history: processed decrypted data saved in {save_path}")
        
def knime_process_predictions():
    print("Knime, process predictions for tickers")
    
    los_angeles_tz = pytz.timezone('America/Los_Angeles')
    
    knime_prediction_output_path = knime_silver_dir / 'predictions'
    knime_prediction_output_path.mkdir(parents=True, exist_ok=True)
    
    knime_api_url = config['paths']['knime_api_url']
    
    payload = {
        "table-input": {
            "table-spec": [
                {"name": "string"},
                {"thing": "string"}
            ],
            "table-data": [
                ["i am", "under water"]
            ]
        }
    }
    
    response = requests.post(
        knime_api_url,
        json=payload,
        auth=HTTPBasicAuth(
            "KMcUlnNMkp7bg2jP5sc4Y9CCwzmLKCHHJvKpnBeg7iY",
            "ZJV54XJd3LvzCy4w4b7RpRjy9V8C0cKLfq3PAwAh-Y8bylsjglNb4DKqvNH4iyNC6whcYPUU3n7l9dHfyGqwCQ"
        )
    )
    
    json_data = response.json()
    print("Status:", response.status_code)
    print("FULL RESPONSE:")
    print(response.text)
    
    rows = json_data["outputValues"]["table-output"]["table-data"]
    print("JSON reponse:")
    print(rows)
    
    print("Launching KNIME workflow...")
    
    print("KNIME workflow finished") 
        
if __name__ == "__main__":
    include_stocks_master = "--stocks-master" in sys.argv
    
    if include_stocks_master:
        knime_process_stocks_master()
        
    knime_process_price_history()
    knime_process_predictions()