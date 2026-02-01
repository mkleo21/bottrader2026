import logging
import os
import azure.functions as func

bp = func.Blueprint()


def connect_db():
    import pyodbc
    conn_str = os.getenv("SQL_CONNECTION_STRING")
    if not conn_str:
        # Local development fallback (Windows)
        return pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=localhost\\SQLEXPRESS;Database=tradecryptoDB;Trusted_Connection=yes;")
    
    # Azure Functions Linux environment comes with ODBC Driver 17 and 18.
    # We use Authentication=ActiveDirectoryMSI for Managed Identity.
    if "Authentication=ActiveDirectoryMSI" not in conn_str:
         # Ensure driver is specified for Linux
         if "DRIVER=" not in conn_str.upper():
             # Driver 18 is preferred on newer Azure environments
             conn_str = "Driver={ODBC Driver 18 for SQL Server};" + conn_str
         
         if "Authentication=" not in conn_str:
             conn_str += "Authentication=ActiveDirectoryMSI;"
             
         # Driver 18 requires TrustServerCertificate=yes if not using a custom cert
         if "TrustServerCertificate=" not in conn_str:
             conn_str += "TrustServerCertificate=yes;"

    return pyodbc.connect(conn_str)


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
MERGE CoinInfoTable AS target
USING (SELECT ? AS CoinSymbol,  ? AS PricePrecision, ? AS QuantityPrecision, ? AS IsActive) AS source
ON (target.CoinSymbol = source.CoinSymbol)
WHEN MATCHED THEN UPDATE SET PricePrecision = source.PricePrecision, QuantityPrecision = source.QuantityPrecision, IsActive = source.IsActive
WHEN NOT MATCHED THEN INSERT (CoinSymbol, PricePrecision, QuantityPrecision, IsActive) VALUES (source.CoinSymbol, source.PricePrecision, source.QuantityPrecision, source.IsActive );
""", sym, pricePrec, qtyPrec, isActive)
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
