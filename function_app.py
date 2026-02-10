import azure.functions as func
import azure.durable_functions as df
from FetchCoinInfoHttp.fetch_coin_info import bp as fetch_coin_bp
from FetchHistoricalDataHttp.fetch_historical_data import bp as fetch_hist_bp
from GetOHLCDataTimer.get_ohlc_data import bp as get_ohlc_bp
from DataCleanerTimer.data_cleaner import bp as data_cleaner_bp
from DoTradeTimer.durable_do_trade import bp as durable_trade_bp

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Register your modular functions here
app.register_functions(fetch_coin_bp)
app.register_functions(fetch_hist_bp)
app.register_functions(get_ohlc_bp)
app.register_functions(data_cleaner_bp)
app.register_functions(durable_trade_bp)