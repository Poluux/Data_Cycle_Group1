-- init.sql

USE [master]
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataCycleProject')
BEGIN
    CREATE DATABASE [DataCycleProject]
END
GO

ALTER DATABASE [DataCycleProject] SET ANSI_NULL_DEFAULT OFF
GO
ALTER DATABASE [DataCycleProject] SET AUTO_CLOSE OFF
GO
ALTER DATABASE [DataCycleProject] SET AUTO_SHRINK OFF
GO
ALTER DATABASE [DataCycleProject] SET AUTO_UPDATE_STATISTICS ON
GO
ALTER DATABASE [DataCycleProject] SET RECOVERY SIMPLE
GO
ALTER DATABASE [DataCycleProject] SET MULTI_USER
GO
ALTER DATABASE [DataCycleProject] SET PAGE_VERIFY CHECKSUM
GO
ALTER DATABASE [DataCycleProject] SET READ_WRITE
GO

USE [DataCycleProject]
GO

-- dimDate
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dimDate' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[dimDate](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [date] [datetime] NULL,
        [year] [int] NULL,
        [month] [int] NULL,
        [day] [int] NULL,
        [day_of_week] [nvarchar](50) NULL,
        [quarter] [int] NULL,
        CONSTRAINT [PK_dimDate] PRIMARY KEY CLUSTERED ([id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY]
END
GO

-- dimTicker
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dimTicker' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[dimTicker](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [ticker] [nvarchar](255) NULL,
        [company_name] [text] NULL,
        [sector] [text] NULL,
        [industry] [text] NULL,
        [currency] [nvarchar](255) NULL,
        [exchange] [nvarchar](255) NULL,
        CONSTRAINT [PK_dimTicker] PRIMARY KEY CLUSTERED ([id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

-- Fact_yfinance
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_yfinance' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[Fact_yfinance](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [tickerDate_FK] [int] NOT NULL,
        [ticker_FK] [int] NOT NULL,
        [ingestionDate_FK] [int] NOT NULL,
        [open] [float] NULL,
        [high] [float] NULL,
        [low] [float] NULL,
        [close] [float] NULL,
        [adjClose] [float] NULL,
        [volume] [bigint] NULL,
        [dividends] [float] NULL,
        [stockSplits] [float] NULL,
        [sessionChange] [float] NULL,
        [sessionChangePCT] [float] NULL,
        [intradayVolatility] [float] NULL,
        CONSTRAINT [PK_Fact_yfinance_1] PRIMARY KEY CLUSTERED
        ([id] ASC, [tickerDate_FK] ASC, [ticker_FK] ASC, [ingestionDate_FK] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY],
        CONSTRAINT [UQ_Fact_yfinance] UNIQUE NONCLUSTERED ([ticker_FK] ASC, [tickerDate_FK] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY]

    ALTER TABLE [dbo].[Fact_yfinance] WITH CHECK ADD
        CONSTRAINT [FK_Fact_yfinance_dimDate_IngestionDate] FOREIGN KEY([ingestionDate_FK])
        REFERENCES [dbo].[dimDate] ([id])

    ALTER TABLE [dbo].[Fact_yfinance] WITH CHECK ADD
        CONSTRAINT [FK_Fact_yfinance_dimDate_TickerDate] FOREIGN KEY([tickerDate_FK])
        REFERENCES [dbo].[dimDate] ([id])

    ALTER TABLE [dbo].[Fact_yfinance] WITH CHECK ADD
        CONSTRAINT [FK_Fact_yfinance_dimTicker] FOREIGN KEY([ticker_FK])
        REFERENCES [dbo].[dimTicker] ([id])
END
GO

-- Fact_Prediction
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_Prediction' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[Fact_Prediction](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [Date_FK] [int] NOT NULL,
        [Ticker_FK] [int] NOT NULL,
        [Ingestion_Date_FK] [int] NOT NULL,
        [PredictedPrice] [float] NULL,
        CONSTRAINT [PK_Fact_Prediction] PRIMARY KEY CLUSTERED
        ([Date_FK] ASC, [Ticker_FK] ASC, [Ingestion_Date_FK] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY]

    ALTER TABLE [dbo].[Fact_Prediction] WITH CHECK ADD
        CONSTRAINT [FK_Fact_Prediction_dimDate] FOREIGN KEY([Date_FK])
        REFERENCES [dbo].[dimDate] ([id])

    ALTER TABLE [dbo].[Fact_Prediction] WITH CHECK ADD
        CONSTRAINT [FK_Fact_Prediction_dimTicker] FOREIGN KEY([Ticker_FK])
        REFERENCES [dbo].[dimTicker] ([id])

    ALTER TABLE [dbo].[Fact_Prediction] WITH CHECK ADD
        CONSTRAINT [FK_Fact_Prediction_Ingestion_Date] FOREIGN KEY([Ingestion_Date_FK])
        REFERENCES [dbo].[dimDate] ([id])
END
GO

-- Fact_TechnicalIndicators
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_TechnicalIndicators' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[Fact_TechnicalIndicators](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [Ticker_FK] [int] NOT NULL,
        [Date_FK] [int] NOT NULL,
        [SMA20] [float] NULL,
        [SMA50] [float] NULL,
        [RSI] [float] NULL,
        [ATR] [float] NULL,
        [MACD] [float] NULL,
        [MACD_Signal] [float] NULL,
        [MACD_Histogram] [float] NULL,
        [BB_Upper] [float] NULL,
        [BB_Middle] [float] NULL,
        [BB_Lower] [float] NULL,
        CONSTRAINT [PK_Fact_TechnicalIndicators] PRIMARY KEY CLUSTERED
        ([id] ASC, [Ticker_FK] ASC, [Date_FK] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY]

    ALTER TABLE [dbo].[Fact_TechnicalIndicators] WITH CHECK ADD
        CONSTRAINT [FK_Fact_TechnicalIndicators_dimDate] FOREIGN KEY([Date_FK])
        REFERENCES [dbo].[dimDate] ([id])

    ALTER TABLE [dbo].[Fact_TechnicalIndicators] WITH CHECK ADD
        CONSTRAINT [FK_Fact_TechnicalIndicators_dimTicker] FOREIGN KEY([Ticker_FK])
        REFERENCES [dbo].[dimTicker] ([id])
END
GO

-- Fact_Audit
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_Audit' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[Fact_Audit](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [Date] [date] NULL,
        [API_Error_Rate] [float] NULL,
        [Missing_Days_Corrected] [int] NULL,
        [Duplicates_Removed] [int] NULL,
        [Data_Quality_Score] [float] NULL,
        CONSTRAINT [PK_Fact_Audit] PRIMARY KEY CLUSTERED ([id] ASC)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
        ON [PRIMARY]
    ) ON [PRIMARY]
END
GO