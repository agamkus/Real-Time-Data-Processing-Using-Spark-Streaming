from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('WARN')	
	
stream1 = spark  \
	.readStream  \
	.format("socket")  \
	.option("host","localhost")  \
	.option("port",12345)  \
	.load()
	
streamDF1 = stream1.selectExpr("value as player")

stream2 = spark  \
	.readStream  \
	.format("socket")  \
	.option("host","localhost")  \
	.option("port",12346)  \
	.load()
	
streamDF2 = stream2.selectExpr("value as person")

# Inner Join Example
joinedDF = streamDF1.join(streamDF2, expr("""player = person"""))

	
query = joinedDF  \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
