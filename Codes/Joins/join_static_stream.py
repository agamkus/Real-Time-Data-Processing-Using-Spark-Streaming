from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


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

streamDF = lines.selectExpr("value as player")
	
mySchema = StructType().add("name", "string").add("age","integer")	
	
staticDF = spark  \
	.read  \
	.option("delimiter",";")  \
	.format("csv")  \
	.schema(mySchema)  \
	.load("testdir/players.csv")


# Inner Join Example
#joinedDF = streamDF.join(staticDF, expr("""player = name"""))


#left outer join
joinedDF = streamDF.join(staticDF, expr("""player = name"""), "left_outer")
	
query = joinedDF  \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
	.start()
	
query.awaitTermination()
