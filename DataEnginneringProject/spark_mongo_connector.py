from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import pandas as pd
import json


# create data frame from json data
def conv_to_df(json_data):
    # this df is for the first key in dict json
    json_data = json.loads(json_data)
    df_1 = pd.DataFrame.from_dict(json_data.get("Meta Data").items())
    headers = df_1.T.iloc[0]
    new_df = df_1.T[1:]
    new_df.columns = headers
    # this df is for 2nd key in dict
    main_keys = []
    value_list = []
    for k, v in json_data.get("Time Series (1min)").items():
        main_keys.append(k)
        value_list.append(v)
        df = pd.DataFrame(value_list)
        df.index = main_keys

    val_2 = new_df["2. Symbol"].values
    df["symbol"] = val_2[0]
    df.columns = ["open", "high", "low", "close", "volume", "symbol"]
    df.reset_index(None, inplace=True)
    df = df.rename(columns={"index": "time_stamp"})
    df.drop_duplicates()
    print(df.head(5))
    return df


# creating spark DataFrame
def to_spark_df(dataframe):
    # mentioning directory for mongo jars
    working_directory = 'jars/*'
    # creating sparkSession with all mongodb configs
    spark = SparkSession.builder.appName("sparkMongoConnector") \
        .config("spark.mongodb.input.uri",
                "mongodb://127.0.0.1/StockMarket_db.new_stocks").config(
        "spark.mongodb.output.uri",
        "mongodb://127.0.0.1/StockMarket_db.new_stocks").config(
        "spark.driver.extraClassPath",
        working_directory).getOrCreate()
    spark_df = spark.createDataFrame(dataframe.astype(str))
    return spark_df


# wtite to mongoDb
def write_to_mongo_db(sparkDf):
    return sparkDf.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()


# creating main func
def main(data):
    data_frame = conv_to_df(data)
    spark_df = to_spark_df(data_frame)
    return write_to_mongo_db(spark_df)


if __name__ == "__main__":

    conf = SparkConf().setAppName("StockData").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # sc = SparkContext(appName="StockData")
    ssc = StreamingContext(sc, 61)
    brokers = "localhost:9092"
    topic = "StockMarkets"
    CompStockDStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    StockValueDStream = CompStockDStream.mapPartitions(lambda value: value[1])
    StockValueDStream.foreachRDD(lambda rdd: rdd.foreach(lambda stocks_data: main(stocks_data)))
    ssc.start()
    ssc.awaitTermination()
