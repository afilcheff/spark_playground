from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
    sparkSession = SparkSession \
        .builder \
        .appName('Checkpointing')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField('Date', TimestampType(), True),
                         StructField('Open', DoubleType(), True),
                         StructField('High', DoubleType(), True),
                         StructField('Low', DoubleType(), True),
                         StructField('Close', DoubleType(), True),
                         StructField('Adjusted Close', DoubleType(), True),
                         StructField('Volume', DoubleType(), True),
                         StructField('Name', StringType(), True)
                         ])

    stockPricesDf = sparkSession \
            .readStream \
            .option('header', 'true') \
            .schema(schema) \
            .csv('./datasets/stock_data')


    print(' ')
    print('Is the stream ready?')
    print(stockPricesDf.isStreaming)


    print(' ')
    print('Schema of the input stream')
    print(stockPricesDf.printSchema())

    stockPricesDf.createOrReplaceTempView('stock_prices')

    avgCloseDf = sparkSession.sql("""SELECT Name, avg(Close)
                                     FROM stock_prices
                                     GROUP BY Name""")  


    query = avgCloseDf \
            .writeStream.outputMode('complete') \
            .format('console') \
            .option('truncate', 'false') \
            .option('numRows', 30) \
            .option('checkpointLocation', 'checkpoint') \
            .start() \
            .awaitTermination()



if __name__ == '__main__':
    main()

