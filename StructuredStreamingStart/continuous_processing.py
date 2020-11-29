from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    sparkSession = SparkSession \
        .builder \
        .appName('Continuous processing') \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    streamDf = sparkSession \
        .readStream \
        .format('rate') \
        .option('rowsPerSecond', 1) \
        .option('rampUpTime', 1) \
        .load()
      

    print(' ')
    print('Is the stream ready?')
    print(streamDf.isStreaming)


    print(' ')
    print('Schema of the input stream')
    print(streamDf.printSchema())


    selectDf = streamDf.selectExpr("*")

    query = selectDf \
        .writeStream \
        .outputMode('append') \
        .format('console') \
        .trigger(continuous='3 second') \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    main()

