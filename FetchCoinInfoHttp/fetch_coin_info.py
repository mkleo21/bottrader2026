import logging
import os
import azure.functions as func

bp = func.Blueprint()


def connect_db():
    import pymssql
    conn_str = os.getenv("SQL_CONNECTION_STRING")
    # If using pymssql, it's better to parse or provide parts. 
    # But pymssql can often take a server string if formatted correctly.
    # For simplicity, we'll assume a standard format or fall back to local for testing.
    # Note: Trusted_Connection=yes won't work on Linux/Azure.
    if not conn_str:
        return pymssql.connect(server='localhost', database='tradecryptoDB')
    
    # Simple parse for typical Azure SQL strings: 
    # e.g. "Server=tcp:myserver.database.windows.net,1433;Database=mydb;User ID=myuser;Password=mypass;"
    parts = {k.strip().upper(): v.strip() for k, v in (p.split('=') for p in conn_str.split(';') if '=' in p)}
    
    server = parts.get('SERVER', '').replace('tcp:', '').split(',')[0]
    database = parts.get('DATABASE', '')
    user = parts.get('USER ID', '')
    password = parts.get('PASSWORD', '')

    return pymssql.connect(server=server, user=user, password=password, database=database)


def fetch_json():
    import requests
    url = os.getenv("COIN_API_URL")
    if url:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    # If no URL configured, return None
    return


def process():
    data = fetch_json()
    #add a check for data being null or empty string
    if not data:
        return {"processed": 0}
    symbols = data.get("Symbols") or data.get("symbols") or []
    #add a check for symbols being null or empty string
    if not symbols:
        return {"processed": 0}
    conn = connect_db()
    cursor = conn.cursor()

    processed = 0

    for s in symbols:
        status = s.get("status") or s.get("Status")
        sym = s.get("symbol") or s.get("Symbol") or s.get("CoinSymbol")
        pricePrec = s.get("pricePrecision") or s.get("price_precision") or 0
        qtyPrec = s.get("quantityPrecision") or s.get("quantity_precision") or 0
        try:
            pricePrec = int(pricePrec)
        except Exception:
            pricePrec = 0
        try:
            qtyPrec = int(qtyPrec)
        except Exception:
            qtyPrec = 0

        #When Status is not TRADING,, then IsActive is set to 0 and Status is TRADING then IsActive is set to 1
        if status != "TRADING":
            isActive = 0
        else:
            isActive = 1
        cursor.execute("""
IF EXISTS (SELECT 1 FROM CoinInfoTable WHERE CoinSymbol = %s)
BEGIN
    UPDATE CoinInfoTable SET PricePrecision = %s, QuantityPrecision = %s, IsActive = %s WHERE CoinSymbol = %s
END
ELSE
BEGIN
    INSERT INTO CoinInfoTable (CoinSymbol, PricePrecision, QuantityPrecision, IsActive) VALUES (%s, %s, %s, %s)
END
""", (sym, pricePrec, qtyPrec, isActive, sym, sym, pricePrec, qtyPrec, isActive))
        processed += 1

    conn.commit()
    cursor.close()
    conn.close()
    return {"processed": processed}


@bp.route(route="FetchCoinInfoHttp", methods=["GET", "POST"])
def FetchCoinInfoHttp(req: func.HttpRequest) -> func.HttpResponse:
    import json
    logging.info("FetchCoinInfo HTTP trigger started.")
    try:
        res = process()
        return func.HttpResponse(json.dumps(res), status_code=200, mimetype="application/json")
    except Exception as e:
        logging.exception("Error fetching coin info: %s", e)
        return func.HttpResponse(str(e), status_code=500)
