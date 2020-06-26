import json
import requests
import config as api_var


class StockData:

    def __init__(self):
        self.api = api_var.API_URL

    def get_data(self, symbol):
        data = {"function": api_var.functions,
                "symbol": symbol,
                "outputsize": api_var.output_size,
                "datatype": api_var.data_type,
                "interval": api_var.interval,
                "apikey": api_var.api_key}

        response = requests.get(self.api, data)
        raw_stat = response.text
        return json.loads(raw_stat)

stocks = StockData()
stock_data = stocks.get_data(api_var.symbol)
# print(stock_data)
