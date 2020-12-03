from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window



if __name__ == "__main__":
    sparkSession = SparkSession \
        .builder \
        .appName("Windowing operations")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("Date", TimestampType(), True),
                         StructField("Open", DoubleType(), True),
                         StructField("High", DoubleType(), True),
                         StructField("Low", DoubleType(), True),
                         StructField("Close", DoubleType(), True),
                         StructField("Adjusted Close", DoubleType(), True),
                         StructField("Volume", DoubleType(), True),
                         StructField("Name", StringType(), True)
                         ])

    stockPricesDf = sparkSession \
            .readStream \
            .option("header", "true") \
            .schema(schema) \
            .csv("datasets/stockPricesDataset/dropLocation")

    print(" ")
    print(stockPricesDf.printSchema())


    averageCloseDf = stockPricesDf \
            .groupBy(window(stockPricesDf.Date, "7 days"), "Name") \
            .agg({"Close": "avg"}) \
            .withColumnRenamed("avg(Close)", "Average Close")


    query = averageCloseDf \
            .writeStream.outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start() \
            .awaitTermination()

