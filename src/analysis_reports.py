import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv
from db_connection import get_engine

def generate_visual_reports():
    reports_dir = "/app/reports" if os.getenv("RUNNING_IN_DOCKER") else "./reports"
    os.makedirs(reports_dir, exist_ok=True)

    print("Extracting data from Gold Layer for Reports...")
    engine = get_engine()

    # DB queries. Date, ticker, close price, daily % change, volume and indicator
    query = """
    SELECT 
        d.date, 
        t.ticker, 
        f.[Close],
        f.sessionChangePCT,
        f.[volume],
        ti.[SMA50]
    FROM Fact_yfinance f
    JOIN dimDate d ON f.TickerDate_FK = d.id
    JOIN DimTicker t ON f.Ticker_FK = t.id
    LEFT JOIN Fact_TechnicalIndicators ti ON ti.Date_FK = d.id AND ti.Ticker_FK = t.id
    WHERE d.date >= '2023-01-01' 
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
    ts_path = os.path.join(reports_dir, '2_Price_Evolution.png')
    plt.savefig(ts_path)
    plt.close()
    print(f"- Saved: {ts_path}")
    # plt.show()


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
    heatmap_path = os.path.join(reports_dir, '1_Correlation_Heatmap.png')
    plt.savefig(heatmap_path)
    plt.close()
    print(f"- Saved: {heatmap_path}")
    # plt.show()


    # TRADING VOLUME PLOT

    print("Generating Trading Volume Plot...")
    plt.figure(figsize=(14, 7))
    # Usamos un gráfico de líneas finas para el volumen temporal
    sns.lineplot(data=df, x='date', y='volume', hue='ticker', linewidth=1.5, alpha=0.8)

    # Styles of the chart
    plt.title('Trading Volume Over Time (2023 - Present)', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Volume (Number of Shares)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title='Ticker')
    plt.tight_layout()

    # Save and show
    vol_path = os.path.join(reports_dir, '3_Trading_Volume.png')
    plt.savefig(vol_path)
    plt.close()
    print(f"- Saved: {vol_path}")
    # plt.show()


    # TECHNICAL INDICATORS PLOT

    print("Generating Technical Indicators Plot...")
    plt.figure(figsize=(14, 7))
    sns.lineplot(data=df, x='date', y='SMA50', hue='ticker', linewidth=2)

    # Styles of the chart
    plt.title('Technical Indicators: 50-Day Simple Moving Average (SMA)', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('SMA 50 Value', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title='Ticker')
    plt.tight_layout()

    # Save and show
    ind_path = os.path.join(reports_dir, '4_SMA50_Trend.png')
    plt.savefig(ind_path)
    plt.close()
    print(f"- Saved: {ind_path}")
    # plt.show()

    print("All reports generated successfully.")