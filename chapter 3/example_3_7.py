from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
	# Create a SparkSession
	spark = (SparkSession
	         .builder
	         .appName("Examole-3_7")
	         .getOrCreate())

	jsonFile = "../data/blogs.json"

	schema = StructType([StructField("Id", IntegerType(), False),
	                     StructField("First", StringType(), False),
	                     StructField("Last", StringType(), False),
	                     StructField("Url", StringType(), False),
	                     StructField("Published", StringType(), False),
	                     StructField("Hits", IntegerType(), False),
	                     StructField("Campaigns", ArrayType(StringType(), True), False)])

	blogsDF = spark.read.schema(schema).json(jsonFile)
	blogsDF.show()
	print(blogsDF.printSchema())
