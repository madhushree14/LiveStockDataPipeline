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

        with requests.get(self.api, data) as response:
            json_data = response.text

        # response = requests.get(self.api, data)
        # json_data = response.text
        return json.loads(json_data)


# dta = StockData()
# mydt = dta.get_data(api_var.symbol)
# print(mydt)