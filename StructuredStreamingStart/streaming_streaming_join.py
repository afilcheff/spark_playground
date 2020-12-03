import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window, current_timestamp



if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("Streaming static joins")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    streaming_schema_1 = StructType([StructField("Country", StringType(), True),\
                                     StructField("Year", StringType(), True),\
                                     StructField("GDP", DoubleType(), True),\
                                     StructField("Population", DoubleType(), True)])

    streamingDf_1 = sparkSession.readStream\
                              .option("header", "true")\
                              .schema(streaming_schema_1)\
                              .csv("datasets/lifeExpectancyDataset/dropLocation_1")


    streamingDf_1.printSchema()


    streaming_schema_2 = StructType([StructField("Country", StringType(), True),\
                                     StructField("Year", StringType(), True),\
                                     StructField("Status", StringType(), True),\
                                     StructField("LifeExpectancy", DoubleType(), True)])


    streamingDf_2 = sparkSession.readStream\
                              .option("header", "true")\
                              .schema(streaming_schema_2)\
                              .csv("datasets/lifeExpectancyDataset/dropLocation_2")

    streamingDf_2.printSchema()



    joinedDf = streamingDf_1.join(streamingDf_2, on=["Country", "Year"])

    selectDf = joinedDf.select("Country", "Year", "GDP", "LifeExpectancy")

    query = selectDf.writeStream\
                    .outputMode("append")\
                    .format("console")\
                    .start()\
                    .awaitTermination()


















