import json
import requests
import config as api_var
from API_call_code import StockData

sampletest = StockData()
ps_list = []
for cmp in api_var.symbol:
    ps = sampletest.get_data(cmp)
    ps_list.append(ps)


print(ps_list)