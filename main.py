from API_call_code import StockData
from kafka_producer import MyKafka
import logging
import time
import config
from logging.config import dictConfig

class Main:

    def __init__(self):
        logging_config = dict(
            version=1,
            formatters={
                'f': {'format':
                          '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
            },
            handlers={
                'h': {'class': 'logging.StreamHandler',
                      'formatter': 'f',
                      'level': logging.DEBUG}
            },
            root={
                'handlers': ['h'],
                'level': logging.DEBUG,
            },
        )

        self.logger = logging.getLogger()
        dictConfig(logging_config)
        self.logger.info("Initializing Kafka Producer")
        self.logger.info("KAFKA_BROKERS={0}".format(config.kafka_broker))
        self.mykafka = MyKafka(config.kafka_broker)

    def init_stock(self):
        self.stock = StockData()
        self.logger.info("StockData Starts Polling Initialized")

    def run(self):
        self.init_stock()
        start_time = time.time()

        while True:
            data = self.stock.get_data(config.symbol)
            self.logger.info("Successfully polled Stock data")
            self.mykafka.send_data(data)
            self.logger.info("Published stock data to Kafka")
            time.sleep(60.0 - ((time.time() - start_time) % 60.0))  # push data to kafka topic in every min



if __name__ == "__main__":
    logging.info("Initializing stock Starts Polling")
    main = Main()
    main.run()


