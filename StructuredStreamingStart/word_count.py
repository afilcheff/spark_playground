
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def main():
	if len(sys.argv) != 3:
		print('Usage: spark-submit WordCount.py <host> <port>',file = sys.stderr)
		exit(-1)

	host = sys.argv[1]
	port = int(sys.argv[2])

	sparkSession = SparkSession \
		.builder \
		.appName("Word Count")\
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")


	readStream = sparkSession \	
		.readStream.format('socket') \
		.option('host', host) \
		.option('port', port) \
		.load()

	print("-------------------------------------------------")	
	print("Streaming source ready: ", readStream.isStreaming)

	readStream.printSchema()

	words = readStream.select( \
		explode(split(readStream.value,' ')).alias('word'))


	wordCounts = words.groupBy('word').count() \
			.orderBy('count')


	query = wordCounts \
		.writeStream \
		.outputMode('complete') \
		.format('console') \
		.start() \
		.awaitTermination()



if __name__ == '__main__':
	main()

