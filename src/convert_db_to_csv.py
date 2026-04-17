import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

SERVER = os.getenv('SQL_SERVER')
DATABASE = os.getenv('SQL_DATABASE')

TABLES = os.getenv('SQL_TABLES').split(',')

base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(base_dir)

output_dir = os.path.join(parent_dir, "data", "gold")

def extract_table(table): 
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'Trusted_Connection=yes;'
    )

    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

def save_csv(df, table):
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, f"data_sac_{table}.csv")
    
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

