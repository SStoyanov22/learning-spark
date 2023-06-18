from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = "`Id` INT, " \
         "`First` STRING," \
         " `Last` STRING, " \
         "`Url` STRING," \
         "`Published` STRING," \
         " `Hits` INT, `Campaigns` " \
         "ARRAY<STRING> "

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
        [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]]

if __name__ == "__main__":
	# Create a SparkSession
	spark = (SparkSession
	         .builder
	         .appName("Examole-3_6")
	         .getOrCreate())

	# Create dataframe using the schema above
	blogs_df = spark.createDataFrame(data, schema)
	# Show the dataframe: it should reflect our table above
	blogs_df.show()
	# Print the schema used by Spark to process the Dataframe
	print(blogs_df.schema)
