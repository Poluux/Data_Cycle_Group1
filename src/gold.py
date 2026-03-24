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

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from encryption import decrypt_table

config_path = os.path.join(base_dir, 'config', 'settings.json')
with open(config_path) as f:
    config = json.load(f)

silver_dir = Path(base_dir) / config['paths']['silver']
ticker_col = config['columns']['ticker']
date_col = config['columns']['date']

load_dotenv()
server = os.getenv('SQL_SERVER')
database = os.getenv('SQL_DATABASE')

if not server or not database:
    raise ValueError("SQL_SERVER and SQL_DATABASE must be set in .env file")

engine = create_engine(
    f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

def get_ticker_id_map():
    """Returns a dict mapping ticker → id from DimTicker"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, ticker FROM DimTicker"))
        return {row.ticker: row.id for row in result}

def get_date_id_map():
    """Returns a dict mapping date → id from dimDate"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, date FROM dimDate"))
        return {pd.to_datetime(row.date).date(): row.id for row in result}

def get_last_date_in_db(table_name, date_fk_col):
    """Get the last date already loaded in the fact table"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT MAX(d.date) 
                FROM {table_name} f
                JOIN dimDate d ON f.{date_fk_col} = d.id
            """))
            last_date = result.scalar()
            return pd.to_datetime(last_date).date() if last_date else None
    except Exception:
        return None

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
    #df = decrypt_table(df)
    df = df[['ticker', 'company_name', 'sector', 'industry', 'currency', 'exchange']]
    df = df.rename(columns={
        'ticker': 'ticker',
        'company_name': 'company_name',
        'sector': 'sector',
        'industry': 'industry',
        'currency': 'currency',
        'exchange': 'exchange'
    })

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
                """), {
                    "company_name": row['company_name'],
                    "sector": row['sector'],
                    "industry": row['industry'],
                    "currency": row['currency'],
                    "exchange": row['exchange'],
                    "ticker": row['ticker']
                })
                print(f"- {row['ticker']}: updated in DimTicker")
            else:
                conn.execute(text("""
                    INSERT INTO DimTicker (ticker, [company_name], sector, industry, currency, exchange)
                    VALUES (:ticker, :company_name, :sector, :industry, :currency, :exchange)
                """), {
                    "ticker": row['ticker'],
                    "company_name": row['company_name'],
                    "sector": row['sector'],
                    "industry": row['industry'],
                    "currency": row['currency'],
                    "exchange": row['exchange']
                })
                print(f"- {row['ticker']}: inserted in DimTicker")

def load_fact_yfinance():
    print("Processing Fact_yfinance")
    silver_path = silver_dir / 'price_history'

    files = list(silver_path.glob('clean_price_history_*.parquet'))
    if not files:
        print("- Fact_yfinance: no files found in Silver")
        return

    # Safety check
    ticker_map = get_ticker_id_map()
    if not ticker_map:
        print("- Fact_yfinance: DimTicker is empty, run with --stocks-master first")
        return

    date_map = get_date_id_map()
    ingestion_date_id = date_map.get(date.today())  # ID pour la date d'ingestion
    if not ingestion_date_id:
        raise ValueError(f"ingestionDate_FK introuvable dans dimDate pour {date.today()}")

    last_date = get_last_date_in_db('Fact_yfinance', 'TickerDate_FK')
    if last_date:
        print(f"- Fact_yfinance: last date in DB: {last_date}")

    for file in files:
        ticker = file.stem.replace('clean_price_history_', '')

        if ticker not in ticker_map:
            print(f"- {ticker}: not found in DimTicker, skipping")
            continue

        df = pd.read_parquet(file)
        #df = decrypt_table(df)

        if last_date:
            df = df[pd.to_datetime(df[date_col]).dt.date > last_date]

        if df.empty:
            print(f"- {ticker}: already up to date")
            continue

        df = df.sort_values(date_col)
        df['daily_return'] = df['adj_close'].pct_change() * 100
        df['cumulative_return'] = (1 + df['adj_close'].pct_change()).cumprod() - 1
        df['cumulative_return'] = df['cumulative_return'] * 100

        df['Ticker_FK'] = ticker_map[ticker]
        df['TickerDate_FK'] = pd.to_datetime(df[date_col]).dt.date.map(date_map)
        df['ingestionDate_FK'] = ingestion_date_id  # <-- rempli maintenant

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
            'stock_splits': 'StockSplits'
        })

        cols = ['Ticker_FK', 'TickerDate_FK', 'ingestionDate_FK',
                'Open', 'High', 'Low', 'Close',
                'AdjClose', 'Volume', 'Dividends', 'StockSplits']
        df = df[[c for c in cols if c in df.columns]]

        df.to_sql('Fact_yfinance', con=engine, if_exists='append', index=False)
        print(f"- {ticker}: {len(df)} new rows inserted into Fact_yfinance")

def load_fact_technical_indicators():
    print("Processing Fact_TechnicalIndicators")
    silver_path = silver_dir / 'price_history'

    files = list(silver_path.glob('clean_price_history_*.parquet'))
    if not files:
        print("- Fact_TechnicalIndicators: no files found in Silver")
        return

    # Safety check
    ticker_map = get_ticker_id_map()
    if not ticker_map:
        print("- Fact_TechnicalIndicators: DimTicker is empty, run with --stocks-master first")
        return

    date_map = get_date_id_map()
    last_date = get_last_date_in_db('Fact_TechnicalIndicators', 'Date_FK')
    if last_date:
        print(f"- Fact_TechnicalIndicators: last date in DB: {last_date}")

    for file in files:
        ticker = file.stem.replace('clean_price_history_', '')

        if ticker not in ticker_map:
            print(f"- {ticker}: not found in DimTicker, skipping")
            continue

        df = pd.read_parquet(file).sort_values(date_col)
        #df = decrypt_table(df)

        # Calculation of technical indicators
        df['SMA20'] = ta.sma(df['adj_close'], length=20)
        df['SMA50'] = ta.sma(df['adj_close'], length=50)
        df['RSI'] = ta.rsi(df['adj_close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['adj_close'], length=14)

        macd = ta.macd(df['adj_close'])
        df['MACD'] = macd['MACD_12_26_9']
        df['MACD_Signal'] = macd['MACDs_12_26_9']
        df['MACD_Histogram'] = macd['MACDh_12_26_9']

        bb = ta.bbands(df['adj_close'])

        # Protection against KeyError : dynamic retrieval of columns
        bb_cols = bb.columns
        df['BB_Upper'] = bb[bb_cols[0]] if len(bb_cols) > 0 else np.nan
        df['BB_Middle'] = bb[bb_cols[1]] if len(bb_cols) > 1 else np.nan
        df['BB_Lower'] = bb[bb_cols[2]] if len(bb_cols) > 2 else np.nan

        # Filtering after last date inserted
        if last_date:
            df = df[pd.to_datetime(df[date_col]).dt.date > last_date]

        if df.empty:
            print(f"- {ticker}: already up to date")
            continue

        df['Ticker_FK'] = ticker_map[ticker]
        df['Date_FK'] = pd.to_datetime(df[date_col]).dt.date.map(date_map)

        # Drop lines with missing dates
        df = df.dropna(subset=['Date_FK'])

        # Replace NaN of technical columns by None for SQL
        technical_cols = ['SMA20', 'SMA50', 'RSI', 'ATR', 
                          'MACD', 'MACD_Signal', 'MACD_Histogram',
                          'BB_Upper', 'BB_Middle', 'BB_Lower']
        df[technical_cols] = df[technical_cols].where(pd.notnull(df[technical_cols]), None)

        # Final column to insert
        cols = ['Ticker_FK', 'Date_FK'] + technical_cols
        df = df[[c for c in cols if c in df.columns]]

        if df.empty:
            print(f"- {ticker}: no valid rows to insert")
            continue

        # Insertion SQL
        df.to_sql('Fact_TechnicalIndicators', con=engine, if_exists='append', index=False)
        print(f"- {ticker}: {len(df)} new rows inserted into Fact_TechnicalIndicators")

if __name__ == "__main__":
    include_stocks_master = "--stocks-master" in sys.argv

    load_dim_date()

    if include_stocks_master:
        load_dim_ticker()

    load_fact_yfinance()
    load_fact_technical_indicators()