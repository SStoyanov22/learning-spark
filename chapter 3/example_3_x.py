from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import os

if __name__ == "__main__":
	# Create a SparkSession
	spark = (SparkSession
	         .builder
	         .appName("Examole-3_x")
	         .getOrCreate())

	fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
	                          StructField('UnitID', StringType(), True),
	                          StructField('IncidentNumber', IntegerType(), True),
	                          StructField('CallType', StringType(), True),
	                          StructField('CallDate', StringType(), True),
	                          StructField('WatchDate', StringType(), True),
	                          StructField('CallFinalDisposition', StringType(), True),
	                          StructField('AvailableDtTm', StringType(), True),
	                          StructField('Address', StringType(), True),
	                          StructField('City', StringType(), True),
	                          StructField('Zipcode', IntegerType(), True),
	                          StructField('Battalion', StringType(), True),
	                          StructField('StationArea', StringType(), True),
	                          StructField('Box', StringType(), True),
	                          StructField('OriginalPriority', StringType(), True),
	                          StructField('Priority', StringType(), True),
	                          StructField('FinalPriority', IntegerType(), True),
	                          StructField('ALSUnit', BooleanType(), True),
	                          StructField('CallTypeGroup', StringType(), True),
	                          StructField('NumAlarms', IntegerType(), True),
	                          StructField('UnitType', StringType(), True),
	                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
	                          StructField('FirePreventionDistrict', StringType(), True),
	                          StructField('SupervisorDistrict', StringType(), True),
	                          StructField('Neighborhood', StringType(), True),
	                          StructField('Location', StringType(), True),
	                          StructField('RowID', StringType(), True),
	                          StructField('Delay', FloatType(), True)])

	# Use the DataFrameReader Interface to read a CSV file
	fire_df = spark.read.csv('../data/sf-fire-calls.csv', header=True, schema=fire_schema)
	parquet_path = '../data/sf-fire-calls.parquet'
	if not os.path.isdir(parquet_path):
		fire_df.write.format('parquet').save(parquet_path)

	#
	few_fire_df = (fire_df \
	               .select('IncidentNumber', 'AvailableDtTm', 'CallType')
	               .where(col('CallType') != 'Medical Incident'))

	few_fire_df.show(5, truncate=False)

	# how many distinct CallTypes were recorded as the causes of the fire calls?
	fire_df \
		.select('CallType') \
		.where(col('CallType').isNotNull()) \
		.agg(countDistinct('CallType').alias('DistinctCallTypes')) \
		.show()

	# list distinct types
	fire_df \
		.select('CallType') \
		.where(col('CallType').isNotNull()) \
		.distinct() \
		.show(10, False)
	# change column name
	new_fire_df = fire_df.withColumnRenamed('Delay', 'ResponseDelayedinMins')
	new_fire_df \
		.select('ResponseDelayedinMins') \
		.where(col('ResponseDelayedinMins') > 5) \
		.show(5, False)

	# set columns from string to date/timestamp
	fire_ts_df = new_fire_df \
		.withColumn('IncidentDate', to_timestamp(col('CallDate'), 'MM/dd/yyyy')) \
		.drop("CallDate") \
		.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")) \
		.drop("WatchDate") \
		.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")) \
		.drop("AvailableDtTm")
	fire_ts_df \
		.select("IncidentDate", "OnWatchDate", "AvailableDtTS") \
		.show(5, False)

	# get distinct years ordered ascending
	fire_ts_df \
		.select(year('IncidentDate')) \
		.distinct() \
		.orderBy(year('IncidentDate')) \
		.show()

	# how many calls were logged for the last seven days
	fire_ts_df \
		.select('IncidentNumber') \
		.where(col('IncidentDate') > date_add(col('IncidentDate'), -7)) \
		.agg(countDistinct('IncidentNumber').alias('DistinctCalls')) \
		.show()

	# most common type of calls
	fire_ts_df \
		.select('CallType') \
		.where(col('CallType').isNotNull()) \
		.groupBy('CallType') \
		.count() \
		.orderBy('count', ascending=False) \
		.show(n=10, truncate=False)

	# show min,max,avg
	fire_ts_df \
		.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"),
	            max("ResponseDelayedinMins")) \
		.show()

	# What were all the different types of fire calls in 2018?
	fire_ts_df \
		.select('CallType') \
		.where((col('CallType').isNotNull()) & (year('IncidentDate') == 2018)) \
		.distinct() \
		.show()

	# What months within the year 2018 saw the highest number of fire calls?
	fire_ts_df \
		.filter(year('IncidentDate') == 2018) \
		.groupBy(month('IncidentDate')) \
		.count() \
		.orderBy('count', ascending=False) \
		.show()

	#['CallNumber', 'UnitID', 'IncidentNumber', 'CallType', 'CallFinalDisposition', 'Address',
	# 'City', 'Zipcode', 'Battalion', 'StationArea', 'Box', 'OriginalPriority', 'Priority',
	# 'FinalPriority', 'ALSUnit', 'CallTypeGroup', 'NumAlarms', 'UnitType', 'UnitSequenceInCallDispatch',
	# 'FirePreventionDistrict', 'SupervisorDistrict', 'Neighborhood', 'Location', 'RowID',
	# 'ResponseDelayedinMins', 'IncidentDate', 'OnWatchDate', 'AvailableDtTS']

	#Which neighborhood in San Francisco generated the most fire calls in 2018?
	fire_ts_df \
		.filter(col('City') == 'San Francisco') \
		.groupBy('Neighborhood') \
		.count() \
		.orderBy('count', ascending=False) \
		.show()

	#Which neighborhoods had the worst response times to fire calls in 2018?
	fire_ts_df \
		.select('Neighborhood', 'ResponseDelayedinMins') \
		.orderBy('ResponseDelayedinMins', ascending=False) \
		.show(10, truncate=False)

	#Is there a correlation between neighborhood, zip code, and number of fire calls?
	fire_cor_df = fire_ts_df \
					.select('Neighborhood', 'Zipcode') \
					.groupBy('Neighborhood', 'Zipcode') \
					.count() \
					.orderBy('count', ascending=False)
	print(fire_cor_df.columns)
	from pyspark.ml.stat import Correlation
	from pyspark.ml.feature import VectorAssembler
	import pandas as pd
	from pyspark.ml.feature import StringIndexer

	#
	indexer = StringIndexer(inputCol="Neighborhood", outputCol="NeighborhoodIndexed")
	fire_cor_indexed_df = indexer.fit(fire_cor_df).transform(fire_cor_df)
	fire_cor_indexed_df = fire_cor_indexed_df.drop('Neighborhood')
	fire_cor_indexed_df = fire_cor_indexed_df.na.drop("any")
	fire_cor_indexed_df.show()
	# convert to vector column first
	vector_col = "corr_features"
	assembler = VectorAssembler(inputCols=fire_cor_indexed_df.columns, outputCol=vector_col)
	df_vector = assembler.transform(fire_cor_indexed_df).select(vector_col)


	# get correlation matrix
	matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
	corrmatrix = matrix.toArray().tolist()
	corr_df = spark.createDataFrame(corrmatrix, fire_cor_indexed_df.columns)
	corr_df.show()


