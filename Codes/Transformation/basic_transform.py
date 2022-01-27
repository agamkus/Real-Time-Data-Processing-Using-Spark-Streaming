from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
	
transformedDF = lines.filter(length(col("value"))>4)
	
query = transformedDF  \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
