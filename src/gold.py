import sys
import os
os.environ["NUMBA_DISABLE_JIT"] = "1"
import json
import pandas as pd
import pandas_ta as ta
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import date
from db_connection import get_engine

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from encryption import decrypt_table

config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)

silver_dir = Path(base_dir) / config['paths']['silver']
ticker_col = config['columns']['ticker']
date_col = config['columns']['date']

# Load depending on context of execution
if os.getenv("RUNNING_IN_DOCKER"):
    load_dotenv(".env",override=True)
else:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"),override=True)

engine = get_engine()

def get_last_date_per_ticker_fact():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                f.Ticker_FK,
                MAX(d.date) as last_date
            FROM Fact_yfinance f
            JOIN dimDate d ON f.TickerDate_FK = d.id
            GROUP BY f.Ticker_FK
        """))
        return {
            row.Ticker_FK: pd.to_datetime(row.last_date).date()
            for row in result if row.last_date
        }


def get_last_date_per_ticker_ti():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                f.Ticker_FK,
                MAX(d.date) as last_date
            FROM Fact_TechnicalIndicators f
            JOIN dimDate d ON f.Date_FK = d.id
            GROUP BY f.Ticker_FK
        """))
        return {
            row.Ticker_FK: pd.to_datetime(row.last_date).date()
            for row in result if row.last_date
        }

def get_ticker_id_map():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, ticker FROM DimTicker"))
        return {row.ticker: row.id for row in result}

def get_date_id_map():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, date FROM dimDate"))
        return {pd.to_datetime(row.date).date(): row.id for row in result}

def load_dim_date(start_year=2015, end_year=2030):
    print("Processing dimDate")

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dimDate")).scalar()

    if count > 0:
        print("- dimDate: already populated, skipping")
        return

    dates = pd.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31", freq='D')
    df = pd.DataFrame({'date': dates})

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['quarter'] = df['date'].dt.quarter
    df['day_of_week'] = df['date'].dt.day_name()
    df['date'] = df['date'].dt.date

    df.to_sql('dimDate', con=engine, if_exists='append', index=False)
    print(f"- dimDate: {len(df)} rows inserted")

def load_dim_ticker():
    print("Processing DimTicker")
    silver_path = silver_dir / 'equity_funds'

    files = sorted(list(silver_path.glob('clean_stocks_master_*.parquet')))
    if not files:
        print("- DimTicker: no files found in Silver")
        return

    df = pd.read_parquet(files[-1])
    df = decrypt_table(df)
    df = df[['ticker', 'company_name', 'sector', 'industry', 'currency', 'exchange']]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            existing = conn.execute(
                text("SELECT id FROM DimTicker WHERE ticker = :ticker"),
                {"ticker": row['ticker']}
            ).scalar()

            if existing:
                conn.execute(text("""
                    UPDATE DimTicker 
                    SET [company_name] = :company_name,
                        sector = :sector,
                        industry = :industry,
                        currency = :currency,
                        exchange = :exchange
                    WHERE ticker = :ticker
                """), row.to_dict())
                print(f"- {row['ticker']}: updated in DimTicker")
            else:
                conn.execute(text("""
                    INSERT INTO DimTicker (ticker, [company_name], sector, industry, currency, exchange)
                    VALUES (:ticker, :company_name, :sector, :industry, :currency, :exchange)
                """), row.to_dict())
                print(f"- {row['ticker']}: inserted in DimTicker")

def load_fact_yfinance():
    print("Processing Fact_yfinance")
    silver_path = silver_dir / 'price_history'

    files = list(silver_path.glob('clean_price_history_*.parquet'))
    if not files:
        print("- Fact_yfinance: no files found in Silver")
        return

    ticker_map = get_ticker_id_map()
    if not ticker_map:
        print("- Fact_yfinance: DimTicker is empty, run with --stocks-master first")
        return

    date_map = get_date_id_map()
    ingestion_date_id = date_map.get(date.today())
    if not ingestion_date_id:
        raise ValueError(f"ingestionDate_FK introuvable dans dimDate pour {date.today()}")

    last_dates = get_last_date_per_ticker_fact()

    for file in files:
        ticker = file.stem.replace('clean_price_history_', '')

        if ticker not in ticker_map:
            print(f"- {ticker}: not found in DimTicker, skipping")
            continue

        ticker_id = ticker_map[ticker]
        last_date = last_dates.get(ticker_id)

        df = pd.read_parquet(file)
        df = decrypt_table(df)

        cols_to_convert = ['open', 'high', 'low', 'close', 'adj_close', 'volume', 'dividends', 'stock_splits']
        for col in cols_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if last_date:
            df = df[pd.to_datetime(df[date_col]).dt.date > last_date]

        if df.empty:
            print(f"- {ticker}: already up to date")
            continue

        df = df.sort_values(date_col)
        df['daily_return'] = df['adj_close'].pct_change() * 100
        df['cumulative_return'] = (1 + df['adj_close'].pct_change()).cumprod() - 1
        df['cumulative_return'] = df['cumulative_return'] * 100

        df['Ticker_FK'] = ticker_id
        df['TickerDate_FK'] = pd.to_datetime(df[date_col]).dt.date.map(date_map)
        df['ingestionDate_FK'] = ingestion_date_id

        missing_dates = df['TickerDate_FK'].isna().sum()
        if missing_dates > 0:
            print(f"- {ticker}: {missing_dates} dates not found in dimDate, skipping those rows")
            df = df.dropna(subset=['TickerDate_FK'])

        df = df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'adj_close': 'AdjClose',
            'volume': 'Volume',
            'dividends': 'Dividends',
            'stock_splits': 'StockSplits',
            'intraday_volatility': 'intradayVolatility',
            'session_change': 'sessionChange',
            'session_change_pct': 'sessionChangePCT'
        })

        cols = ['Ticker_FK', 'TickerDate_FK', 'ingestionDate_FK',
                'Open', 'High', 'Low', 'Close',
                'AdjClose', 'Volume', 'Dividends', 'StockSplits', 'intradayVolatility', 'sessionChange', 'sessionChangePCT']
        df = df[[c for c in cols if c in df.columns]]

        
        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=['Ticker_FK', 'TickerDate_FK', 'ingestionDate_FK'])

        df.to_sql('Fact_yfinance', con=engine, if_exists='append', index=False)
        print(f"- {ticker}: {len(df)} new rows inserted into Fact_yfinance")


def load_fact_technical_indicators():
    print("Processing Fact_TechnicalIndicators")
    silver_path = silver_dir / 'price_history'

    files = list(silver_path.glob('clean_price_history_*.parquet'))
    if not files:
        print("- Fact_TechnicalIndicators: no files found in Silver")
        return

    ticker_map = get_ticker_id_map()
    if not ticker_map:
        print("- Fact_TechnicalIndicators: DimTicker is empty, run with --stocks-master first")
        return

    date_map = get_date_id_map()

    last_dates = get_last_date_per_ticker_ti()

    for file in files:
        ticker = file.stem.replace('clean_price_history_', '')

        if ticker not in ticker_map:
            print(f"- {ticker}: not found in DimTicker, skipping")
            continue

        ticker_id = ticker_map[ticker]
        last_date = last_dates.get(ticker_id)

        df = pd.read_parquet(file)
        df = decrypt_table(df)
        df = df.sort_values(date_col)

        cols_to_convert = ['open', 'high', 'low', 'close', 'adj_close', 'volume', 'dividends', 'stock_splits']
        for col in cols_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['SMA20'] = ta.sma(df['adj_close'], length=20)
        df['SMA50'] = ta.sma(df['adj_close'], length=50)
        df['RSI'] = ta.rsi(df['adj_close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['adj_close'], length=14)

        macd = ta.macd(df['adj_close'])
        df['MACD'] = macd['MACD_12_26_9']
        df['MACD_Signal'] = macd['MACDs_12_26_9']
        df['MACD_Histogram'] = macd['MACDh_12_26_9']

        bb = ta.bbands(df['adj_close'])
        bb_cols = bb.columns
        df['BB_Lower'] = bb[bb_cols[0]] if len(bb_cols) > 0 else np.nan
        df['BB_Middle'] = bb[bb_cols[1]] if len(bb_cols) > 1 else np.nan
        df['BB_Upper'] = bb[bb_cols[2]] if len(bb_cols) > 2 else np.nan

        if last_date:
            df = df[pd.to_datetime(df[date_col]).dt.date > last_date]

        if df.empty:
            print(f"- {ticker}: already up to date")
            continue

        df['Ticker_FK'] = ticker_id
        df['Date_FK'] = pd.to_datetime(df[date_col]).dt.date.map(date_map)

        df = df.dropna(subset=['Date_FK'])

        technical_cols = ['SMA20', 'SMA50', 'RSI', 'ATR', 
                          'MACD', 'MACD_Signal', 'MACD_Histogram',
                          'BB_Upper', 'BB_Middle', 'BB_Lower']
        df[technical_cols] = df[technical_cols].where(pd.notnull(df[technical_cols]), None)

        cols = ['Ticker_FK', 'Date_FK'] + technical_cols
        df = df[[c for c in cols if c in df.columns]]

        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=['Ticker_FK', 'Date_FK'])

        if df.empty:
            print(f"- {ticker}: no valid rows to insert")
            continue

        df.to_sql('Fact_TechnicalIndicators', con=engine, if_exists='append', index=False)
        print(f"- {ticker}: {len(df)} new rows inserted into Fact_TechnicalIndicators")

if __name__ == "__main__":
    include_stocks_master = "--stocks-master" in sys.argv

    load_dim_date()

    if include_stocks_master:
        load_dim_ticker()

    load_fact_yfinance()
    load_fact_technical_indicators()