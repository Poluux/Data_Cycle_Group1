import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

SERVER = os.getenv('SQL_SERVER')
DATABASE = os.getenv('SQL_DATABASE')

TABLES = os.getenv('SQL_TABLES').split(',')

def extract_table(table): 
    conn = pyodbc.connect(
        f'DRIVER={{SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'Trusted_Connection=yes;'
    )

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

def save_csv(df, table):
    os.makedirs("data/gold", exist_ok=True)
    
    file_path = f"data/gold/data_sac_{table}.csv"
    
    if not df.empty:
        df.to_csv(file_path, index=False)
        print(f"{table} saved successfully")
    else:
        print(f"{table} is empty — table not updated")

def export_sql_to_csv():
    for table in TABLES:
        df = extract_table(table)
        save_csv(df, table)

if __name__ == "__main__":
    export_sql_to_csv()