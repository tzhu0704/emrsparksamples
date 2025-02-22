import sys
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

# spark = SparkSession.builder.appName("icebergjob").getOrCreate()

# spark = (SparkSession.builder.config("spark.hadoop.hive.metastore.client.factory.class",
#                                      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate())
spark = SparkSession.builder \
        .appName("Iceberg Glue PySpark Demo") \
        .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "software.amazon.awssdk:s3:2.20.0,"
            "software.amazon.awssdk:sts:2.20.0,"
            "software.amazon.awssdk:kms:2.20.0,"
            "software.amazon.awssdk:glue:2.20.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://emr-eks-spark-us-east-1-509399592849/example-prefix/") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport() \
        .getOrCreate()

DOC_EXAMPLE_BUCKET = sys.argv[1]
print("-------"+DOC_EXAMPLE_BUCKET )
# print("S3 bucket name: {}".format(DOC_EXAMPLE_BUCKET))

# nyTaxi = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[2])

# updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now()))

# updatedNYTaxi.printSchema()

# print(updatedNYTaxi.show())

# print("Total number of records: " + str(updatedNYTaxi.count()))
## Create a DataFrame
data = spark.createDataFrame([
 ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
 ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
 ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
 ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z")
],["id", "creation_date", "last_update_time"])

## Write a DataFrame as a Iceberg dataset to the S3 location
spark.sql("""CREATE TABLE IF NOT EXISTS glue_catalog.default.myiceberg_demo (id string,
creation_date string,
current_date string)
USING iceberg
location '{}/db/myiceberg_demo'""".format(DOC_EXAMPLE_BUCKET))



