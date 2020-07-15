""" API vars """
API_URL = "https://www.alphavantage.co/query"

functions = "TIME_SERIES_INTRADAY"
symbol = ["SAP", "SIEGY", "AAPL", "AMZN"]
output_size = "full"
data_type = "json"
interval = "1min"
api_key = "T51VUT68CNYXA"

""" Kafka configs """
kafka_broker = "34.72.28.214:9092"
topic = "StockMarkets"

