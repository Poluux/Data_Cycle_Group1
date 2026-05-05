import pandas as pd
import os
from dotenv import load_dotenv
from db_connection import get_connection

load_dotenv()

TABLES = os.getenv("SQL_TABLES").split(",")
TABLES_TO_MERGE = os.getenv("SQL_TABLES_TO_MERGE").split(",")

DATE_KEYS = {
    "Fact_Prediction": "Date_FK",
    "Fact_TechnicalIndicators": "Date_FK",
    "Fact_yfinance": "tickerDate_FK",
}

base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(base_dir)
output_dir = os.path.join(parent_dir, "data", "gold")


def extract_table(table):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df


def merge_date(df, dim_date, fact_key):
    dim_date_filtered = dim_date[["id", "date"]]
    return df.merge(dim_date_filtered, left_on=fact_key, right_on="id", how="left")


def save_csv(df, table):
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"data_sac_{table}.csv")
    if not df.empty:
        df.to_csv(file_path, index=False)


def export_sql_to_csv():
    dim_date = extract_table("dimDate")
    save_csv(dim_date, "dimDate")

    for table in TABLES:
        if table == "dimDate":
            continue
        df = extract_table(table)
        save_csv(df, table)

    for table in TABLES_TO_MERGE:
        df = extract_table(table)
        fact_key = DATE_KEYS.get(table)
        if fact_key:
            df_merged = merge_date(df, dim_date, fact_key)
            save_csv(df_merged, table)


if __name__ == "__main__":
    export_sql_to_csv()
