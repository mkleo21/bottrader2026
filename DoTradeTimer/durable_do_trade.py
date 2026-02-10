import logging
import json
import os
import time
import azure.functions as func
import azure.durable_functions as df
import pandas as pd
from datetime import timedelta, datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException
from shared.db_utils import get_db_connection, db_session, ensure_order_book_table
from shared.indicators import calculate_indicators
from shared.email_utils import send_email_alert

# Durable Function Blueprint
bp = df.Blueprint()

# Trading Constants
DEFAULT_LEVERAGE = 5
MAX_SLIPPAGE_PCT = 0.01
LIMIT_ORDER_OFFSET = 0.01

# Retry Options for Activities
default_retry_options = df.RetryOptions(
    first_retry_interval_in_milliseconds=5000,
    max_number_of_attempts=3
)

def get_binance_client():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    testnet = os.getenv("BINANCE_USE_TESTNET", "False").lower() == "true"
    return Client(api_key, api_secret, testnet=testnet)

# --- STARTER ---
@bp.timer_trigger(arg_name="timer", schedule="0 10 0,4,8,12,16,20 * * *")
@bp.durable_client_input(client_name="client")
async def DoTradeStarter(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    logging.info("DoTrade Starter triggered by timer.")
    ensure_order_book_table()
    instance_id = await client.start_new("DoTradeOrchestrator", None)
    logging.info(f"Started orchestration with ID = '{instance_id}'.")

# --- ORCHESTRATOR ---
@bp.orchestration_trigger(context_name="context")
def DoTradeOrchestrator(context: df.DurableOrchestrationContext):
    signals = yield context.call_activity_with_retry("GetSignalsActivity", default_retry_options, None)
    
    if not signals:
        return "No signals found."

    tasks = []
    for signal in signals:
        # Pass signal data to sub-orchestrator
        tasks.append(context.call_sub_orchestrator("TradeExecutionOrchestrator", signal))
    
    yield context.task_all(tasks)
    return f"Processed {len(signals)} signals."

@bp.orchestration_trigger(context_name="context")
def TradeExecutionOrchestrator(context: df.DurableOrchestrationContext):
    signal = context.get_input()
    symbol = signal['CoinSymbol']
    
    # 1. Prepare Trade (Check existing pos, Set margin, place limit orders, log to OrderBook)
    trade_info = yield context.call_activity_with_retry("PrepareTradeActivity", default_retry_options, signal)
    
    if not trade_info or not trade_info.get('order_placed'):
        return f"Trade skipped for {symbol}: {trade_info.get('reason', 'Unknown')}"

    # 2. Entry Wait Cycle (3m + 3m)
    fire_at = context.current_utc_datetime + timedelta(minutes=3)
    yield context.create_timer(fire_at)
    
    is_filled = yield context.call_activity_with_retry("CheckPositionActivity", default_retry_options, symbol)
    if not is_filled:
        fire_at = context.current_utc_datetime + timedelta(minutes=3)
        yield context.create_timer(fire_at)
        is_filled = yield context.call_activity_with_retry("CheckPositionActivity", default_retry_options, symbol)

    if not is_filled:
        yield context.call_activity_with_retry("CancelTradeActivity", default_retry_options, trade_info)
        return f"Entry timed out for {symbol}."

    # 3. Finalize Selection (TP/SL)
    yield context.call_activity_with_retry("FinalizeTradeEntryActivity", default_retry_options, trade_info)
    
    # 4. MONITORING LOOP
    start_time = context.current_utc_datetime
    exit_type = None
    
    while True:
        # Wait until next monitoring window (XX:15 to allow OHLC update to finish)
        now = context.current_utc_datetime
        next_hour = ((now.hour // 4) + 1) * 4
        if next_hour >= 24:
            next_check = now.replace(day=now.day+1, hour=0, minute=15, second=0, microsecond=0)
        else:
            next_check = now.replace(hour=next_hour, minute=15, second=0, microsecond=0)
            
        yield context.create_timer(next_check)

        # Activity: Monitor Status
        status = yield context.call_activity_with_retry("MonitorStatusActivity", default_retry_options, symbol)
        
        # Condition 4: TP/SL Detection (Position closed externally)
        if not status['is_open']:
            exit_type = yield context.call_activity_with_retry("DetectTPSLExitActivity", default_retry_options, symbol)
            break

        # Condition 1 & 2: Zscore Exits
        z = status['zscore']
        if signal['TradeOrderPriceDirection'] == 'LONG':
            if z < -2.0: exit_type = 'Level2'
            elif z > -0.25: exit_type = 'Level0'
        else: # SHORT
            if z > 2.0: exit_type = 'Level2'
            elif z < 0.25: exit_type = 'Level0'

        # Condition 3: Time Exit (12 Hours)
        elapsed_hours = (context.current_utc_datetime - start_time).total_seconds() / 3600
        if not exit_type and elapsed_hours >= 12:
            exit_type = 'TimeExit'

        if exit_type:
            yield context.call_activity_with_retry("ClosePositionActivity", default_retry_options, symbol)
            break

    # 5. Final Result Reporting
    result = yield context.call_activity_with_retry("UpdateOrderBookFinalActivity", default_retry_options, {
        'order_db_id': trade_info['order_db_id'],
        'exit_type': exit_type,
        'symbol': symbol,
        'entry_time': trade_info['entry_time_str']
    })
    
    return f"Completed {symbol} with {exit_type}. P/L: {result.get('pnl')}"

# --- ACTIVITIES ---

@bp.activity_trigger(input_name="none")
def GetSignalsActivity(none):
    with db_session() as cursor:
        cursor.execute("EXEC GetActiveTradingSignals")
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

@bp.activity_trigger(input_name="signal")
def PrepareTradeActivity(signal):
    symbol = signal['CoinSymbol']
    signal_price = float(signal['CurrentPrice'])
    client = get_binance_client()
    try:
        # 0. Robustness: Skip if position already exists for this symbol
        positions = client.futures_position_information(symbol=symbol)
        pos = next((i for i in positions if i["symbol"] == symbol), None)
        if pos and abs(float(pos['positionAmt'])) > 0:
            return {'order_placed': False, 'reason': f'Position already open for {symbol}'}

        # 1. Set Margin Type to ISOLATED
        try:
            client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
        except BinanceAPIException as e:
            # Handle delisting check
            if -1121 == e.code or "Invalid symbol" in e.message:
                logging.error(f"Symbol {symbol} appears to be delisted. Deactivating.")
                with db_session() as cursor_d:
                    cursor_d.execute("UPDATE CoinInfoTable SET IsActive = 0, DeactivationReason = 'Delisted / Invalid Symbol detected during trade setup' WHERE CoinSymbol = ?", (symbol,))
                return {'order_placed': False, 'reason': 'Delisted symbol'}
            if "No need to change" not in str(e):
                logging.warning(f"Error setting margin type for {symbol}: {e}")

        # 2. Check current price and slippage
        ticker = client.futures_symbol_ticker(symbol=symbol)
        m_price = float(ticker['price'])
        diff = (m_price - signal_price) / signal_price
        
        if (signal['TradeOrderPriceDirection'] == 'LONG' and diff < -MAX_SLIPPAGE_PCT) or \
           (signal['TradeOrderPriceDirection'] == 'SHORT' and diff > MAX_SLIPPAGE_PCT):
            return {'order_placed': False, 'reason': 'Slippage Check Failed'}

        # 3. Get Precision & Balance & Log to OrderBook
        entry_time = datetime.utcnow()
        with db_session() as cursor:
            cursor.execute("SELECT PricePrecision, QuantityPrecision FROM CoinInfoTable WHERE CoinSymbol = ?", (symbol,))
            row = cursor.fetchone()
            p_prec, q_prec = (row[0], row[1]) if row else (2, 2)
            
            client.futures_change_leverage(symbol=symbol, leverage=DEFAULT_LEVERAGE)
            balance = float(client.futures_account()['totalMarginBalance'])
            alloc = float(os.getenv("TRADE_ALLOCATION_PCT", "0.1"))
            qty = (balance * alloc * DEFAULT_LEVERAGE) / signal_price / 2
            
            cursor.execute("""
                INSERT INTO OrderBook (CoinSymbol, TradeDirection, SignalPrice, Quantity, TargetPrice, StopLossPrice, Status, EntryTimeMetadata)
                VALUES (?, ?, ?, ?, ?, ?, 'Attempted', ?)
            """, symbol, signal['TradeOrderPriceDirection'], signal_price, qty * 2, float(signal['TargetPrice']), float(signal['StopLossPrice']), entry_time)
            cursor.execute("SELECT SCOPE_IDENTITY()")
            db_id = int(cursor.fetchone()[0])

        # 5. Place 2 Limit Orders
        side = Client.SIDE_BUY if signal['TradeOrderPriceDirection'] == 'LONG' else Client.SIDE_SELL
        q_str = "{:0.{}f}".format(qty, q_prec)
        client.futures_create_order(symbol=symbol, side=side, type='LIMIT', timeInForce='GTC', quantity=q_str, price="{:0.{}f}".format(signal_price, p_prec))
        p2 = signal_price * (0.99 if signal['TradeOrderPriceDirection'] == 'LONG' else 1.01)
        client.futures_create_order(symbol=symbol, side=side, type='LIMIT', timeInForce='GTC', quantity=q_str, price="{:0.{}f}".format(p2, p_prec))

        return {
            'order_placed': True, 'order_db_id': db_id, 'symbol': symbol, 
            'direction': signal['TradeOrderPriceDirection'], 'target_price': signal['TargetPrice'],
            'stop_loss': signal['StopLossPrice'], 'p_prec': p_prec, 'q_prec': q_prec,
            'entry_time_str': entry_time.isoformat()
        }
    except Exception as e:
        logging.error(f"Error in PrepareTradeActivity for {symbol}: {e}")
        send_email_alert(f"Error in PrepareTradeActivity: {symbol}", str(e), "SystemError")
        return {'order_placed': False, 'reason': str(e)}

@bp.activity_trigger(input_name="symbol")
def CheckPositionActivity(symbol):
    try:
        pos = next(i for i in get_binance_client().futures_position_information(symbol=symbol) if i["symbol"] == symbol)
        return abs(float(pos['positionAmt'])) > 0
    except: return False

@bp.activity_trigger(input_name="trade_info")
def FinalizeTradeEntryActivity(trade_info):
    client = get_binance_client()
    symbol = trade_info['symbol']
    side = Client.SIDE_SELL if trade_info['direction'] == 'LONG' else Client.SIDE_BUY
    p_prec = trade_info['p_prec']
    
    client.futures_create_order(symbol=symbol, side=side, type='TAKE_PROFIT_MARKET', stopPrice="{:0.{}f}".format(float(trade_info['target_price']), p_prec), closePosition=True)
    client.futures_create_order(symbol=symbol, side=side, type='STOP_MARKET', stopPrice="{:0.{}f}".format(float(trade_info['stop_loss']), p_prec), closePosition=True)
    
    with db_session() as cursor:
        cursor.execute("UPDATE OrderBook SET Status = 'Filled' WHERE OrderID = ?", trade_info['order_db_id'])
    send_email_alert(f"Trade Entry: {symbol}", f"Position is open for {symbol}. TP/SL set.", "TradeEntry")

@bp.activity_trigger(input_name="trade_info")
def CancelTradeActivity(trade_info):
    client = get_binance_client()
    client.futures_cancel_all_open_orders(symbol=trade_info['symbol'])
    with db_session() as cursor:
        cursor.execute("UPDATE OrderBook SET Status = 'Cancelled' WHERE OrderID = ?", trade_info['order_db_id'])
    send_email_alert(f"Trade Cancelled: {trade_info['symbol']}", "Entry orders were not filled.", "TradeCancelled")

@bp.activity_trigger(input_name="symbol")
def MonitorStatusActivity(symbol):
    with db_session() as cursor:
        cursor.execute("SELECT TOP 1 Zscore FROM FourHour WHERE CoinSymbol = ? ORDER BY PriceDateTime DESC", (symbol,))
        row = cursor.fetchone()
        z = row[0] if row else 0

    client = get_binance_client()
    pos = next(i for i in client.futures_position_information(symbol=symbol) if i["symbol"] == symbol)
    is_open = abs(float(pos['positionAmt'])) > 0
    
    return {'is_open': is_open, 'zscore': z}

@bp.activity_trigger(input_name="symbol")
def DetectTPSLExitActivity(symbol):
    client = get_binance_client()
    # Looking back at recent trades for PnL
    trades = client.futures_account_trades(symbol=symbol, limit=2)
    if trades:
        last_pnl = sum(float(t['realizedPnl']) for t in trades)
        return 'TP' if last_pnl > 0 else 'SL'
    return 'TP/SL'

@bp.activity_trigger(input_name="symbol")
def ClosePositionActivity(symbol):
    client = get_binance_client()
    client.futures_cancel_all_open_orders(symbol=symbol)
    pos = next(i for i in client.futures_position_information(symbol=symbol) if i["symbol"] == symbol)
    qty = float(pos['positionAmt'])
    if abs(qty) > 0:
        side = Client.SIDE_SELL if qty > 0 else Client.SIDE_BUY
        client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=abs(qty))
    return True

@bp.activity_trigger(input_name="trade_result_data")
def UpdateOrderBookFinalActivity(trade_result_data):
    client = get_binance_client()
    symbol = trade_result_data['symbol']
    entry_ts = int(datetime.fromisoformat(trade_result_data['entry_time']).timestamp() * 1000)
    
    # Precise P&L: Fetch trades since entry_time
    trades = client.futures_account_trades(symbol=symbol, startTime=entry_ts)
    
    total_pnl = sum(float(t['realizedPnl']) for t in trades)
    exit_price = float(trades[-1]['price']) if trades else 0
    exit_time = datetime.utcnow()

    with db_session() as cursor:
        cursor.execute("""
            UPDATE OrderBook 
            SET Status = 'Profit/Loss', ExitType = ?, ExitPrice = ?, ProfitLoss = ?, EntryTime = ?, ExitTime = ?, UpdatedTimestamp = GETDATE()
            WHERE OrderID = ?
        """, trade_result_data['exit_type'], exit_price, total_pnl, datetime.fromisoformat(trade_result_data['entry_time']), exit_time, trade_result_data['order_db_id'])
    
    send_email_alert(f"Trade Closed: {symbol}", f"Exit Type: {trade_result_data['exit_type']}\nProfit/Loss: {total_pnl}\nExit Price: {exit_price}", "TradeClosed")
    return {'pnl': total_pnl}
