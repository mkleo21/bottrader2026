import azure.functions as func
from FetchCoinInfoHttp.fetch_coin_info import bp as fetch_coin_bp

app = func.FunctionApp()

# Register your modular functions here
app.register_functions(fetch_coin_bp)