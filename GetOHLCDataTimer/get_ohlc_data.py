import logging
import azure.functions as func
import pandas as pd
import asyncio
import time
from binance import AsyncClient, BinanceSocketManager
from shared.db_utils import get_db_connection, db_session, ensure_four_hour_table
from shared.indicators import calculate_indicators, bulk_insert_four_hour_data
from shared.email_utils import send_email_alert

bp = func.Blueprint()

def get_active_coins():
    """Fetches all active coins from CoinInfoTable."""
    try:
        with db_session() as cursor:
            cursor.execute("SELECT CoinSymbol FROM CoinInfoTable WHERE IsActive = 1")
            return [row.CoinSymbol for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Database error in get_active_coins: {e}")
        return []

def get_historical_context(symbol, limit=200):
    """Fetches the last N records from FourHour table for indicator context."""
    with db_session() as cursor:
        query = f"SELECT TOP {limit} OpenPrice, HighPrice, LowPrice, ClosePrice, CoinVolume, PriceDateTime FROM FourHour WHERE CoinSymbol = ? ORDER BY PriceDateTime DESC"
        cursor.execute(query, (symbol,))
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([list(row) for row in rows], columns=['Open', 'High', 'Low', 'Close', 'Volume', 'PriceDateTime'])
        df = df.sort_values('PriceDateTime').reset_index(drop=True)
        return df

async def fetch_and_process_socket(symbols):
    """Opens WebSocket connection and waits for the 4H candle to close for each symbol."""
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    
    streams = [f"{s.lower()}@kline_4h" for s in symbols]
    processed_symbols = {}
    pending_symbols = set(symbols)
    
    logging.info(f"Starting WebSocket for {len(symbols)} symbols...")
    
    async with bm.multiplex_socket(streams) as stream:
        while pending_symbols:
            try:
                # User requirement: wait for a maximum of 3 minutes (180 seconds)
                res = await asyncio.wait_for(stream.recv(), timeout=180)
                
                if not res:
                    break
                    
                data = res['data']['k']
                symbol = res['stream'].split('@')[0].upper()
                
                if data['x']: # candle closed
                    logging.info(f"Candle closed for {symbol}")
                    
                    volume = float(data['v'])
                    
                    # Deactivate if volume is 0
                    if volume == 0:
                        logging.warning(f"Deactivating {symbol} due to 0 volume in GetOHLCData.")
                        with db_session() as cursor:
                            cursor.execute("UPDATE CoinInfoTable SET IsActive = 0, DeactivationReason = 'Zero Volume in real-time candle' WHERE CoinSymbol = ?", (symbol,))
                        
                        if symbol in pending_symbols:
                            pending_symbols.remove(symbol)
                        continue # Skip further processing for this symbol and wait for the next message

                    # Proceed with normal processing
                    context_df = get_historical_context(symbol)
                    
                    new_row = {
                        'Open': float(data['o']),
                        'High': float(data['h']),
                        'Low': float(data['l']),
                        'Close': float(data['c']),
                        'Volume': float(data['v']),
                        'PriceDateTime': pd.to_datetime(data['t'], unit='ms')
                    }
                    
                    if not context_df.empty and new_row['PriceDateTime'] <= context_df['PriceDateTime'].max():
                        if symbol in pending_symbols:
                            pending_symbols.remove(symbol)
                        continue

                    full_df = pd.concat([context_df, pd.DataFrame([new_row])], ignore_index=True)
                    full_df = calculate_indicators(full_df)
                    final_row = full_df.iloc[-1]
                    
                    processed_symbols[symbol] = {
                        'CoinSymbol': symbol,
                        'OpenPrice': final_row['Open'],
                        'ClosePrice': final_row['Close'],
                        'HighPrice': final_row['High'],
                        'LowPrice': final_row['Low'],
                        'CoinVolume': final_row['Volume'],
                        'PriceDateTime': final_row['PriceDateTime'],
                        'RSI': final_row['RSI'] if not pd.isna(final_row['RSI']) else None,
                        'ATR': final_row['ATR'] if not pd.isna(final_row['ATR']) else None,
                        'AverageVolume': final_row['AverageVolume'] if not pd.isna(final_row['AverageVolume']) else None,
                        'ADX': final_row['ADX'] if not pd.isna(final_row['ADX']) else None,
                        'Zscore': final_row['Zscore'] if not pd.isna(final_row['Zscore']) else None
                    }
                    if symbol in pending_symbols:
                        pending_symbols.remove(symbol)
                        
            except asyncio.TimeoutError:
                logging.warning("WebSocket timed out (3 mins) waiting for data.")
                send_email_alert("Binance Data Timeout", f"No data received from Binance via WebSocket within 3 minutes for active coins. Pending: {list(pending_symbols)}", "BinanceDataTimeout")
                break
            except Exception as e:
                logging.error(f"Error in WebSocket loop: {e}")
                break
                
    await client.close_connection()
    return list(processed_symbols.values())

@bp.timer_trigger(arg_name="timer", schedule="0 58 3,7,11,15,19,23 * * *")
def GetOHLCData(timer: func.TimerRequest) -> None:
    logging.info("GetOHLCData timer trigger started.")
    
    ensure_four_hour_table()
    
    # Retry logic for fetching coin list
    symbols = get_active_coins()
    if not symbols:
        logging.info("Coin list empty, retrying in 5 seconds...")
        time.sleep(5)
        symbols = get_active_coins()

    if not symbols:
        logging.error("No active coins found after retry. Sending email.")
        send_email_alert("No Coins List", "The CoinInfoTable returned no active coins (IsActive=1) after a retry. Please check the database.", "NoActiveCoins")
        return

    # Run WebSocket loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    new_records = loop.run_until_complete(fetch_and_process_socket(symbols))
    
    if new_records:
        bulk_insert_four_hour_data(new_records)
        logging.info(f"Successfully processed {len(new_records)} records.")
    else:
        logging.info("No new records processed.")
