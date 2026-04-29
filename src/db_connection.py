# src/db.py
import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine

if os.getenv("RUNNING_IN_DOCKER"):
    load_dotenv(".env", override=True)
else:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"),override=True)

SERVER   = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
PASSWORD = os.getenv("SQL_PASSWORD")

if not SERVER or not DATABASE:
    raise ValueError("SQL_SERVER and SQL_DATABASE must be set in .env file")

if os.getenv("RUNNING_IN_DOCKER") and not PASSWORD:
    raise ValueError("SQL_PASSWORD must be set in .env file when running in Docker")

def get_connection():
    """Return a row pyodb connection"""
    if os.getenv("RUNNING_IN_DOCKER"):
        return pyodbc.connect(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={SERVER},1433;'
            f'DATABASE={DATABASE};'
            f'UID=sa;PWD={PASSWORD};'
            f'TrustServerCertificate=yes;'
        )
    else:
        return pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'Trusted_Connection=yes;'
        )

def get_engine():
    """Return an sqlalchemy engine"""
    if os.getenv("RUNNING_IN_DOCKER"):
        return create_engine(
            f"mssql+pyodbc://sa:{PASSWORD}@{SERVER}:1433/{DATABASE}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    else:
        return create_engine(
            f"mssql+pyodbc://{SERVER}/{DATABASE}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )