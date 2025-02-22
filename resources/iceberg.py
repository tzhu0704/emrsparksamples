import sys
from datetime import datetime
# import time
from time import sleep
from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("icebergjob").getOrCreate()

spark = (SparkSession.builder.config("spark.hadoop.hive.metastore.client.factory.class",
                                     "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate())

DOC_EXAMPLE_BUCKET = sys.argv[1]

print("S3 bucket name: {}".format(DOC_EXAMPLE_BUCKET))

current_datetime = datetime.now()
formatted_date = current_datetime.strftime("%Y-%m-%d")
formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")

print("格式化后的日期和时间：", formatted_date)

# Create a DataFrame
data = spark.createDataFrame([
 ("5011", "2024-12-24", "2024-11-07T13:51:39.340396Z"),
 ("5011", "2024-12-24", "2024-11-07T12:14:58.597216Z"),
 ("5021", "2024-12-24", "2024-11-07T13:51:40.417052Z"),
 ("5031", "2024-12-24", "2024-11-01T13:51:40.519832Z")
],["id", "creation_date", "last_update_time"])



## Write a DataFrame as a Iceberg dataset to the S3 location
spark.sql("""CREATE TABLE IF NOT EXISTS glue_catalog.default.iceberg_table (id string,
creation_date string,
last_update_time string)
USING iceberg
location '{}/example-prefix/db/iceberg_table'""".format(DOC_EXAMPLE_BUCKET))

data.writeTo("glue_catalog.default.iceberg_table").append()

df = spark.read.format("iceberg").load("glue_catalog.default.iceberg_table")
df.show()