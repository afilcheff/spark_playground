import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split



def main():
	if len(sys.argv) != 3:
		print('Usage: spark-submit happiness_score.py <host> <port>',file = sys.stderr)
		exit(-1)

	host = sys.argv[1]
	port = int(sys.argv[2])

	sparkSession = SparkSession\
		.builder\
		.appName('Calculating average happiness score')\
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")

	readStream = sparkSession\
			.readStream\
			.format('socket')\
			.option('host', host)\
			.option('port', port)\
			.load()

	print("-------------------------------------------------")	
	print("Streaming source ready: ", readStream.isStreaming)

	readStreamDf = readStream.selectExpr("split(value, ' ')[0] as Country",\
										 "split(value, ' ')[1] as Region", \
										 "split(value, ' ')[2] as HappinessScore")

	readStreamDf.createOrReplaceTempView('happiness')

	
	averageScoreDf = sparkSession.sql("""SELECT Region, AVG(HappinessScore) as avgScore
								         FROM Happiness GROUP BY Region""")
	

	query = averageScoreDf \
		.writeStream \
		.outputMode('complete') \
		.format('console') \
		.start() \
		.awaitTermination()


if __name__ == '__main__':
	main()