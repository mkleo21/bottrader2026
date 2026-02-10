import logging
import os
import azure.functions as func
from shared.db_utils import get_db_connection, db_session, ensure_coin_info_table
from shared.email_utils import send_email_alert

bp = func.Blueprint()

def fetch_json():
    import requests
    url = os.getenv("COIN_API_URL")
    if url:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    return

def get_existing_coins():
    """Fetches all existing coin symbols from the database."""
    with db_session() as cursor:
        cursor.execute("SELECT CoinSymbol FROM CoinInfoTable")
        return {row.CoinSymbol for row in cursor.fetchall()}

def process():
    data = fetch_json()
    if not data:
        return []
    
    symbols = data.get("Symbols") or data.get("symbols") or []
    if not symbols:
        return []

    existing_coins = get_existing_coins()
    new_coins_added = []

    with db_session() as cursor:
        for s in symbols:
            sym = s.get("symbol") or s.get("Symbol") or s.get("CoinSymbol")
            
            # Only process symbols ending with USDT and NOT already in DB
            if not sym or not sym.endswith("USDT") or sym in existing_coins:
                continue

            status = s.get("status") or s.get("Status")
            pricePrec = int(s.get("pricePrecision") or s.get("price_precision") or 0)
            qtyPrec = int(s.get("quantityPrecision") or s.get("quantity_precision") or 0)
            isActive = 1 if status == "TRADING" else 0

            # Only INSERT, no updates per user requirement
            cursor.execute("""
                INSERT INTO CoinInfoTable (CoinSymbol, PricePrecision, QuantityPrecision, IsActive) 
                VALUES (?, ?, ?, ?)
            """, sym, pricePrec, qtyPrec, isActive)
            
            new_coins_added.append(sym)

    return new_coins_added

@bp.timer_trigger(arg_name="timer", schedule="0 30 0 30 * *")
def FetchCoinInfoTimer(timer: func.TimerRequest) -> None:
    logging.info("FetchCoinInfo Timer trigger started.")
    try:
        ensure_coin_info_table()
        new_coins = process()
        if new_coins:
            subject = "New Coins Added"
            body = f"The following {len(new_coins)} new coins were added to the database:\n\n" + "\n".join(new_coins)
            send_email_alert(subject, body, "NewCoinsAdded")
            logging.info(f"Added {len(new_coins)} new coins and sent notification.")
        else:
            logging.info("No new coins found to add.")
    except Exception as e:
        logging.exception("Error in FetchCoinInfoTimer: %s", e)
        send_email_alert("Error in FetchCoinInfoTimer", str(e), "SystemError")
