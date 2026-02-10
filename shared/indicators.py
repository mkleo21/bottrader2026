import pandas as pd
import pandas_ta as ta
import logging
from shared.db_utils import get_db_connection, db_session

def calculate_indicators(df):
    """
    Calculates technical indicators for the given dataframe.
    Expected columns in df: 'Open', 'High', 'Low', 'Close', 'Volume'
    """
    if df.empty:
        return df

    # RSI
    df['RSI'] = ta.rsi(df['Close'], length=14)
    
    # ATR
    df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
    
    # Average Volume (e.g., 20 period SMA of volume)
    df['AverageVolume'] = ta.sma(df['Volume'], length=20)
    
    # ADX
    adx_df = ta.adx(df['High'], df['Low'], df['Close'], length=14)
    if adx_df is not None:
        df['ADX'] = adx_df['ADX_14']
    else:
        df['ADX'] = None
        
    # Z-Score (e.g., of Close price over 20 periods)
    df['Zscore'] = ta.zscore(df['Close'], length=20)
    
    return df

def bulk_insert_four_hour_data(data_list):
    """
    Inserts a list of dictionaries into the FourHour table in bulk.
    """
    if not data_list:
        return

    # SQL query for insertion
    sql = """
        INSERT INTO FourHour (
            CoinSymbol, OpenPrice, ClosePrice, HighPrice, LowPrice, 
            CoinVolume, PriceDateTime, RSI, ATR, AverageVolume, ADX, Zscore
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    params = [
        (
            d['CoinSymbol'], d['OpenPrice'], d['ClosePrice'], d['HighPrice'], d['LowPrice'],
            d['CoinVolume'], d['PriceDateTime'], d['RSI'], d['ATR'], d['AverageVolume'], d['ADX'], d['Zscore']
        )
        for d in data_list
    ]
    
    with db_session() as cursor:
        cursor.executemany(sql, params)
        logging.info("Successfully inserted %d records into FourHour table.", len(data_list))
