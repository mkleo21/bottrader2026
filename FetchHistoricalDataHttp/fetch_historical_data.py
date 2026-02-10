import logging
import os
import azure.functions as func
import pandas as pd
from binance.client import Client
from datetime import datetime
from shared.db_utils import get_db_connection, db_session, ensure_four_hour_table
from shared.indicators import calculate_indicators, bulk_insert_four_hour_data
from shared.email_utils import send_email_alert

bp = func.Blueprint()

def get_binance_client():
    """Returns a Binance client using environment variables."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    testnet = os.getenv("BINANCE_USE_TESTNET", "False").lower() == "true"
    return Client(api_key, api_secret, testnet=testnet)

def get_active_coins():
    """Fetches all active coins from CoinInfoTable."""
    try:
        with db_session() as cursor:
            cursor.execute("SELECT CoinSymbol FROM CoinInfoTable WHERE IsActive = 1")
            return [row.CoinSymbol for row in cursor.fetchall()]
    except Exception as e:
        logging.exception("Error fetching active coins: %s", e)
        return []

def get_latest_data_times():
    """Returns a dictionary of {CoinSymbol: latest_PriceDateTime}."""
    try:
        with db_session() as cursor:
            cursor.execute("SELECT CoinSymbol, MAX(PriceDateTime) as LastTime FROM FourHour GROUP BY CoinSymbol")
            return {row.CoinSymbol: row.LastTime for row in cursor.fetchall()}
    except Exception as e:
        logging.exception("Error fetching latest data times: %s", e)
        return {}

def fetch_binance_historical_data(symbol, latest_time=None):
    """
    Fetches historical data for a symbol.
    If latest_time is provided, fetches from latest_time (with 200 bars context).
    Otherwise, fetches exactly 200 closed bars.
    """
    client = get_binance_client()
    try:
        if latest_time:
            # Fetch from latest_time minus context (200 bars * 4H)
            start_ts = int((latest_time - pd.Timedelta(hours=200 * 4)).timestamp() * 1000)
            klines = client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_4HOUR,
                startTime=start_ts
            )
        else:
            # New coin: fetch 201 to get 200 closed
            klines = client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_4HOUR,
                limit=201
            )

        if not klines or len(klines) < 2:
            return pd.DataFrame()

        # Drop the last candle as it is the current running candle
        klines = klines[:-1]
        
        # If no latest_time, keep only 200
        if not latest_time:
            klines = klines[-200:]

        df = pd.DataFrame(klines, columns=[
            'OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume',
            'CloseTime', 'QuoteAssetVolume', 'NumberofTrades',
            'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore'
        ])
        
        df['PriceDateTime'] = pd.to_datetime(df['OpenTime'], unit='ms')
        df['Open'] = df['Open'].astype(float)
        df['High'] = df['High'].astype(float)
        df['Low'] = df['Low'].astype(float)
        df['Close'] = df['Close'].astype(float)
        df['Volume'] = df['Volume'].astype(float)
        
        return df[['PriceDateTime', 'Open', 'High', 'Low', 'Close', 'Volume']]
    except Exception as e:
        logging.error(f"Error fetching Binance data for {symbol}: {e}")
        return pd.DataFrame()

@bp.timer_trigger(arg_name="timer", schedule="0 45 0 30 * *")
def FetchHistoricalDataTimer(timer: func.TimerRequest) -> None:
    logging.info("FetchHistoricalData Timer trigger started.")
    ensure_four_hour_table()

    symbols = get_active_coins()
    if not symbols:
        logging.info("No symbols to process.")
        return

    latest_times = get_latest_data_times()
    all_processed_data = []

    for symbol in symbols:
        last_time = latest_times.get(symbol)
        df = fetch_binance_historical_data(symbol, latest_time=last_time)
        
        if df.empty:
            continue
            
        # Check for zero volume (indicating inactive or delisted coin)
        if df['Volume'].sum() == 0:
            logging.info(f"Deactivating {symbol} due to zero volume.")
            with db_session() as cursor:
                cursor.execute("UPDATE CoinInfoTable SET IsActive = 0, DeactivationReason = 'Zero Volume detected in historical sync' WHERE CoinSymbol = ?", (symbol,))
            continue

        # Calculate indicators on the fetched block (includes context if last_time exists)
        df = calculate_indicators(df)
        
        # Filter: only keep records newer than what we have
        if last_time:
            df = df[df['PriceDateTime'] > last_time]
            
        if df.empty:
            continue

        for _, row in df.iterrows():
            record = {
                'CoinSymbol': symbol,
                'OpenPrice': row['Open'],
                'ClosePrice': row['Close'],
                'HighPrice': row['High'],
                'LowPrice': row['Low'],
                'CoinVolume': row['Volume'],
                'PriceDateTime': row['PriceDateTime'],
                'RSI': row['RSI'] if not pd.isna(row['RSI']) else None,
                'ATR': row['ATR'] if not pd.isna(row['ATR']) else None,
                'AverageVolume': row['AverageVolume'] if not pd.isna(row['AverageVolume']) else None,
                'ADX': row['ADX'] if not pd.isna(row['ADX']) else None,
                'Zscore': row['Zscore'] if not pd.isna(row['Zscore']) else None
            }
            all_processed_data.append(record)
            
    if all_processed_data:
        bulk_insert_four_hour_data(all_processed_data)
        send_email_alert("Historical Data Fetch Complete", f"Successfully synced {len(all_processed_data)} new records for {len(symbols)} coins.", "HistoricalDataSummary")
    else:
        send_email_alert("Historical Data Fetch Complete", "Database is already up to date. No new records fetched.", "HistoricalDataNoNew")
