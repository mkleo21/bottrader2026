# FetchHistoricalData Azure Function Walkthrough

I have implemented the `FetchHistoricalData` function and its supporting modules. As discussed, I used **Python** for this implementation to leverage standard data analysis libraries and maintain consistency with your existing project.

## Key Changes

### 1. New Timer Function: `FetchCoinInfoTimer`
- **Path**: [FetchCoinInfoHttp/fetch_coin_info.py](file:///d:/Mateen/projects/2026/bot-trader/FetchCoinInfoHttp/fetch_coin_info.py)
- **Schedule**: `0 30 0 30 * *` (30th of every month at 12:30 AM UTC).
- **Features**:
    - **Incremental Sync**: Only inserts *new* coins found on Binance. Existing coins are not updated.
    - **Notification**: Sends an email via Azure Communication Services with the list of newly added coin symbols.

### 2. New Timer Function: `FetchHistoricalDataTimer`
- **Path**: [FetchHistoricalDataHttp/fetch_historical_data.py](file:///d:/Mateen/projects/2026/bot-trader/FetchHistoricalDataHttp/fetch_historical_data.py)
- **Schedule**: `0 45 0 30 * *` (30th of every month at 12:45 AM UTC).
- **Features**:
    - **Delta Sync Logic**: 
        - If the coin exists in the database, it fetches only the missing data since the last record (up to the latest closed 4H candle).
        - It pulls 200 bars of historical context for *every* fetch to ensure technical indicators (RSI, ADX, etc.) are precise.
        - If no records exist (e.g., for a newly added coin), it fetches the initial 200 bars.
    - **Notification**: Sends a summary email upon completion, including how many new records were synced.

### 2. Common Indicator Module
- **Path**: [shared/indicators.py](file:///d:/Mateen/projects/2026/bot-trader/shared/indicators.py)
- **Features**:
    - Centralized logic using `pandas_ta`.
    - `bulk_insert_four_hour_data` function using `pyodbc`'s `executemany` for high performance.
    - Automatic table creation logic (`ensure_four_hour_table`).

### 3. New Timer Function: `GetOHLCData`
- **Path**: [GetOHLCDataTimer/get_ohlc_data.py](file:///d:/Mateen/projects/2026/bot-trader/GetOHLCDataTimer/get_ohlc_data.py)
- **Schedule**: `0 58 3,7,11,15,19,23 * * *` (UTC).
- **Features**:
    - **WebSocket Integration**: Uses `BinanceSocketManager` to listen for real-time 4H klines.
    - **Error Handling & Notifications**:
        - **DB Retry**: If the coin list is empty, it waits 5s and retries once.
        - **Email Alerts**: Sends an email via Azure Communication Services if no coins are found after retry OR if the WebSocket times out (3 minutes).
    - **Context-Aware Indicators**: Fetches the last 200 bars for contextual indicator accuracy.

### 5. Email Utility
- **Path**: [shared/email_utils.py](file:///d:/Mateen/projects/2026/bot-trader/shared/email_utils.py)
- **Features**:
    - Generic `send_email_alert` function using your Azure Communication Services credentials.

### 3. New Execution & Monitoring Engine: `DoTrade` (Durable)
- **Path**: [DoTradeTimer/durable_do_trade.py](file:///d:/Mateen/projects/2026/bot-trader/DoTradeTimer/durable_do_trade.py)
- **Schedule**: `0 10 0,4,8,12,16,20 * * *` (UTC).
- **Features**:
    - **Durable Lifecycle**: Manages the entire trade from entry signals to final exit monitoring in a single orchestration.
    - **Advanced Exit Monitoring**: After a trade is filled, the bot enters a 4-hourly monitoring loop to check:
        - **Level 2 Exit**: Closes if price overextends (Long Zscore < -2, Short Zscore > 2).
        - **Level 0 Exit**: Closes when price returns to neutral (Long Zscore > -0.25, Short Zscore < 0.25).
        - **Time Exit**: Forced closure after **12 hours** of holding.
        - **TP/SL Detection**: Automatically detects and logs if automated Take Profit or Stop Loss orders were triggered.
    - **Automated Logging**: Calculates final **Profit/Loss** and updates the `OrderBook` table with the `ExitType` and actual execution price.
    - **Email Summary**: Sends a final notification detailing the trade result (Profit or Loss) and the reason for exit.
    - **Production Robustness**:
        - **Duplicate Prevention**: Automatically skips trade entry if a position is already open for that currency.
        - **Delisted Coin Handling**: Automatically deactivates coins in your database if Binance reports them as delisted or "Invalid Symbol".
        - **Precise P&L**: Calculates Profit/Loss by fetching only the trade history between the *exact* entry and exit timestamps.
        - **Resilient Retries**: Each trade step (Binance orders, DB updates) is protected by a retry policy to handle minor network blips.
    - **Asset Hygiene**:
        - **Zero Volume Deactivation**: If a coin reports 0 volume during either historical sync or real-time OHLC updates, it is automatically marked as inactive.
        - **Deactivation Tracking**: A new `DeactivationReason` column in `CoinInfoTable` tracks exactly why a coin was disabled (e.g., "Zero Volume", "Delisted").
    - **Signal Archiving**:
        - **SignalsTable**: Every time the signal SP is called, valid candidate signals are archived in this table for historical reference.
        - **Deduplication**: Built-in logic ensures that multiple calls within the same 4H window do not create duplicate signal entries.

### 7. New Maintenance Function: `DataCleaner`
- **Path**: [DataCleanerTimer/data_cleaner.py](file:///d:/Mateen/projects/2026/bot-trader/DataCleanerTimer/data_cleaner.py)
- **Schedule**: `0 0 2 * * 0` (Weekly at Sunday 2 AM UTC).
- **Features**:
    - **Self-Cleaning**: Uses a SQL CTE to keep only the most recent 200 records per coin.
    - **Summary Email**: Sends a status email notifying how many records were deleted.

### 8. Database Infrastructure & Optimization
- **Path**: [shared/db_utils.py](file:///d:/Mateen/projects/2026/bot-trader/shared/db_utils.py)
- **Features**:
    - **Context Manager (`db_session`)**: Automated database connection and cursor management. Ensures connections are always closed and transactions are committed/rolled back safely.
    - **Centralized Schema Management**: All table creation logic (`ensure_coin_info_table`, `ensure_four_hour_table`, etc.) is now centralized here.
    - **Performance Toggle**: Uses `CheckDBTablesExists` environment variable. If set to `false`, the bot skips table existence checks on startup, reducing cold-start latency and database overhead.

### 9. CI/CD Deployment Enhancements
- **Path**: [.github/workflows/master_functionappswedencentral.yml](file:///d:/Mateen/projects/2026/bot-trader/.github/workflows/master_functionappswedencentral.yml)
- **Features**:
    - **Dependency Bundling**: Automatically packages `.python_packages` into the deployment artifact, ensuring all libraries (like `pandas-ta` and `azure-functions-durable`) are available in the Azure runtime.

### 6. Project Configuration & Registration
- **Registered**: Added to [function_app.py](file:///d:/Mateen/projects/2026/bot-trader/function_app.py).
- **Dependencies**: Updated [requirements.txt](file:///d:/Mateen/projects/2026/bot-trader/requirements.txt) with `azure-communication-email`.

## Configuration & Alerts

### Environment Variables
The following environment variables must be configured in your `local.settings.json` (for local development) or in Azure Function App Configuration (for production).

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SQL_CONNECTION_STRING` | Connection string for Azure SQL Database. | - |
| `BINANCE_API_KEY` | API Key for Binance Futures (Testnet or Mainnet). | - |
| `BINANCE_API_SECRET` | API Secret for Binance Futures. | - |
| `BINANCE_USE_TESTNET` | Set to `true` to use the Binance Testnet. | `false` |
| `CheckDBTablesExists` | Toggles table existence checks on startup. | `true` |
| `COIN_API_URL` | Endpoint for Binance exchange info metadata. | `.../exchangeInfo` |

### Email Alerts Table
The system automatically sends email notifications via Azure Communication Services. You can control each alert individually using the environment variables listed below (set to `false` to disable).

| Short Name | Description | Source | Control Parameter (Key) |
| :--- | :--- | :--- | :--- |
| **New Coins Added** | List of new symbols found and synced. | `FetchCoinInfo` | `Email_NewCoinsAdded` |
| **Fetch Summary**| Details of successful historical sync. | `HistoricalData` | `Email_HistoricalDataSummary` |
| **DB Up-to-Date** | Confirms all coins are already current. | `HistoricalData` | `Email_HistoricalDataNoNew` |
| **Data Timeout** | Alerted if WebSocket data fails for 3 mins. | `GetOHLCData` | `Email_BinanceDataTimeout` |
| **No Active Coins**| Warning if DB has no active coins (IsActive=1). | `GetOHLCData` | `Email_NoActiveCoins` |
| **Trade Entry** | Notification when a new position opens. | `DoTrade` | `Email_TradeEntry` |
| **Trade Cancelled**| Alert if entry signal did not fill. | `DoTrade` | `Email_TradeCancelled` |
| **Trade Closed** | Final summary P/L and Exit Reason. | `DoTrade` | `Email_TradeClosed` |
| **Cleanup Done** | Summary of old record removal. | `DataCleaner` | `Email_CleanupDone` |
| **System Error** | Detailed logs if any background process fails. | Various | `Email_SystemError` |

## Verification Steps

### 1. Environment Configuration
Ensure your `local.settings.json` includes:
```json
{
  "Values": {
    "SQL_CONNECTION_STRING": "...",
    "CheckDBTablesExists": "true"
  }
}
```

### 2. Install Dependencies
Run the following in your terminal:
```bash
pip install -r requirements.txt
```

### 3. Start Azure Functions Host
```bash
func start
```

### 4. Verify Database Infrastructure
1.  **Automated Creation**: Run `FetchCoinInfo` or `GetOHLCData`. The tables will be created automatically if they don't exist.
2.  **Verify Schema**: Check `CoinInfoTable` for the new `DeactivationReason` column (`VARCHAR(200)`).
3.  **Performance Check**: Set `CheckDBTablesExists` to `false` and verify that database connections are no longer opened for schema checks on startup.

### 5. Verify P/L Logging
Check the `OrderBook` table after a trade closes:
```sql
SELECT TOP 10 * FROM OrderBook ORDER BY UpdatedTimestamp DESC
```

> [!IMPORTANT]
> All database interactions now use a safe context manager pattern, preventing connection leaks and ensuring transaction integrity.
