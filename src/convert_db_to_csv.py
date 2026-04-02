import pandas as pd
import pyodbc

server = 'WIN-F77IL0T7HKL\\MSSQLSERVER01'
database = 'DataCycleProject'

tables = ["dimDate", "dimTicker", "Fact_Prediction", "Fact_TechnicalIndicators", "Fact_yfinance"]

conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'Trusted_Connection=yes;'
)

for table in tables:
    query = f"SELECT * FROM {table}"

    df = pd.read_sql(query, conn)

    file_path = f"dados_sac_{table}.csv"
    df.to_csv(f"data/gold/{file_path}", index=False)

    print(f"CSV da tabela {table} atualizada com sucesso!")

conn.close()