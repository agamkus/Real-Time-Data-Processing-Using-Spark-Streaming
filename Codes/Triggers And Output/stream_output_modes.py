from pyspark.sql import SparkSession

spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()

spark.sparkContext.setLogLevel('WARN')
	
lines = spark  \
	.readStream  \
	.format("socket")  \
	.option("host","localhost")  \
	.option("port",12345)  \
	.load()
	
query = lines  \
	.writeStream  \
	.outputMode("update")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
