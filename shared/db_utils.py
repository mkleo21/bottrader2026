import os
import pyodbc
import logging
from contextlib import contextmanager

def get_db_connection():
    """
    Creates and returns a connection to the Azure SQL database 
    using the connection string from environment variables.
    """
    conn_str = os.getenv("SQL_CONNECTION_STRING")
    if not conn_str:
        logging.error("SQL_CONNECTION_STRING environment variable is not set.")
        raise ValueError("SQL_CONNECTION_STRING environment variable is not set.")
    
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        logging.exception("Failed to connect to the database: %s", e)
        raise

@contextmanager
def db_session():
    """
    Context manager for database connections and cursors.
    Automatically closes the cursor and connection.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def _should_check_tables():
    """Returns True if CheckDBTablesExists is 'true' (case-insensitive) or not set."""
    val = os.getenv("CheckDBTablesExists", "true").lower()
    return val == "true"

def ensure_coin_info_table():
    """
    Checks if the CoinInfoTable exists and creates it if not.
    """
    if not _should_check_tables():
        return
        
    with db_session() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CoinInfoTable')
            BEGIN
                CREATE TABLE CoinInfoTable (
                    CoinID INT IDENTITY(1,1) PRIMARY KEY,
                    CoinSymbol VARCHAR(20) NOT NULL,
                    PricePrecision INT NULL,
                    QuantityPrecision INT NULL,
                    IsActive BIT NOT NULL,
                    DeactivationReason VARCHAR(200),
                    CreatedAt DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
                    UpdatedAt DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME()
                )
            END
        """)

def ensure_four_hour_table():
    """
    Checks if the FourHour table exists and creates it if not.
    """
    if not _should_check_tables():
        return

    with db_session() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FourHour')
            BEGIN
                CREATE TABLE FourHour (
                    RecordID INT IDENTITY(1,1) PRIMARY KEY,
                    CoinSymbol VARCHAR(20),
                    OpenPrice FLOAT,
                    ClosePrice FLOAT,
                    HighPrice FLOAT,
                    LowPrice FLOAT,
                    CoinVolume FLOAT,
                    PriceDateTime DATETIME,
                    RSI FLOAT,
                    ATR FLOAT,
                    AverageVolume FLOAT,
                    ADX FLOAT,
                    Zscore FLOAT,
                    CreatedDateTime DATETIME DEFAULT GETDATE()
                )
            END
        """)

def ensure_order_book_table():
    """
    Checks if the OrderBook table exists and creates it if not.
    """
    if not _should_check_tables():
        return

    with db_session() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'OrderBook')
            BEGIN
                CREATE TABLE OrderBook (
                    OrderID INT IDENTITY(1,1) PRIMARY KEY,
                    CoinSymbol VARCHAR(20),
                    TradeDirection VARCHAR(10),
                    SignalPrice FLOAT,
                    Quantity FLOAT,
                    TargetPrice FLOAT,
                    StopLossPrice FLOAT,
                    Status VARCHAR(20),
                    StatusMessage NVARCHAR(MAX),
                    ExitType VARCHAR(20),
                    ExitPrice FLOAT,
                    ProfitLoss FLOAT,
                    EntryTime DATETIME,
                    ExitTime DATETIME,
                    TradeTimestamp DATETIME DEFAULT GETDATE(),
                    UpdatedTimestamp DATETIME DEFAULT GETDATE()
                )
            END
        """)

def ensure_signals_table():
    """
    Checks if the SignalsTable exists and creates it if not.
    """
    if not _should_check_tables():
        return

    with db_session() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SignalsTable')
            BEGIN
                CREATE TABLE SignalsTable (
                    SignalID INT IDENTITY(1,1) PRIMARY KEY,
                    CoinSymbol VARCHAR(20),
                    PriceDateTime DATETIME,
                    Zscore FLOAT,
                    RSI FLOAT,
                    ADX FLOAT,
                    Direction VARCHAR(10),
                    CurrentPrice FLOAT,
                    TargetPrice FLOAT,
                    StopLossPrice FLOAT,
                    CreatedDateTime DATETIME DEFAULT GETDATE(),
                    CONSTRAINT UC_Signal UNIQUE (CoinSymbol, PriceDateTime)
                )
            END
        """)
