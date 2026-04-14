import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
reports_dir = os.path.join(base_dir, 'reports')
os.makedirs(reports_dir, exist_ok=True)

# Connect to the SQL Server from gold layer
env_path = os.path.join(base_dir, 'src', '.env')
load_dotenv(dotenv_path=env_path)
server = os.getenv('SQL_SERVER')
database = os.getenv('SQL_DATABASE')

print(f"DEBUG - Connecting to Server: {server} | Database: {database}")

if not server or not database:
    raise ValueError("ERROR: Las variables SQL_SERVER o SQL_DATABASE están vacías. Revisa tu archivo src/.env")

engine = create_engine(
    f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

print("Extracting data from Gold Layer...")

# DB queries. Date, ticker, close price and daily % change
query = """
SELECT 
    d.date, 
    t.ticker, 
    f.[Close],
    f.sessionChangePCT
FROM Fact_yfinance f
JOIN dimDate d ON f.TickerDate_FK = d.id
JOIN DimTicker t ON f.Ticker_FK = t.id
WHERE d.date >= '2023-01-01' -- Filtramos los últimos años para mayor claridad visual
"""

df = pd.read_sql(query, engine)
df['date'] = pd.to_datetime(df['date'])

print(f"Data loaded successfully: {len(df)} rows.")


# TIME SERIES PLOT 

print("Generating Time Series Plot...")
plt.figure(figsize=(14, 7))
sns.lineplot(data=df, x='date', y='Close', hue='ticker', linewidth=2)

# Styles of the chart
plt.title('Time Series: Stock Price Evolution (2023 - Present)', fontsize=16, fontweight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Close Price (USD)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(title='Ticker')
plt.tight_layout()

# Save and show
ts_path = os.path.join(reports_dir, 'time_series_plot.png')
plt.savefig(ts_path)
print(f"- Saved: {ts_path}")
plt.show()


# CORRELATION HEATMAP 

print("Generating Correlation Heatmap...")
# Pivot the table: dates are rows and tickers are columns
df_pivot = df.pivot(index='date', columns='ticker', values='sessionChangePCT').dropna()

# Calculate the correlation matrix
corr_matrix = df_pivot.corr()

plt.figure(figsize=(8, 6))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0, 
            linewidths=0.5, fmt='.2f', square=True)

# Styles of the chart
plt.title('Correlation Heatmap: Daily Returns', fontsize=16, fontweight='bold')
plt.tight_layout()

# Save and show
heatmap_path = os.path.join(reports_dir, 'correlation_heatmap.png')
plt.savefig(heatmap_path)
print(f"- Saved: {heatmap_path}")
plt.show()