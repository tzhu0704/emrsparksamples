import sys
from datetime import datetime
# import time
from time import sleep
from pyspark.sql import SparkSession

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Initialize Spark Session with Iceberg support
# spark = (SparkSession.builder
#          .config("spark.hadoop.hive.metastore.client.factory.class",
#                  "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
#          .enableHiveSupport()
#          .getOrCreate())


spark = (SparkSession.builder
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3")
         .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
         .config("spark.sql.catalog.s3tablesbucket.warehouse","arn:aws:s3tables:us-east-1:509399592849:bucket/my-s3-tablebucket0")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.hadoop.hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
         .config("hive.metastore.glue.catalogid", "509399592849") \
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.glue_catalog.warehouse", "s3://emr-eks-spark-us-east-1-509399592849/example-prefix/")
         .enableHiveSupport().getOrCreate())

def transfer_iceberg_table(dest_bucket_name, source_table_name, dest_table_name, source_db_name, dest_db_name,transfertype):
    try:
        # Read from source Iceberg table
       
        if transfertype.lower() =='s3tables' :

            logger.info(f"""create namespace if not exists s3tablesbucket.{dest_db_name}""")
            spark.sql(f"""create namespace if not exists s3tablesbucket.{dest_db_name}""")
            # spark.sql(f"""show namespaces in demoblog""").show()
            # Create new Iceberg table in destination
            logger.info(f"reading source S3 table...glue_catalog.{source_db_name}.{source_table_name}")
           
            source_df = spark.read.format("iceberg").load(f"glue_catalog.{source_db_name}.{source_table_name}")

            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS s3tablesbucket.{dest_db_name}.{dest_table_name}
            USING iceberg
            AS SELECT * FROM glue_catalog.{source_db_name}.{source_table_name} WHERE 1=0
            """)

            logger.info(f"Created destination S3 table...{transfertype}") 

           
            # Write data to destination table
            table_store = f"s3tablesbucket.{dest_db_name}.{dest_table_name}"
            logger.info(f"table_store: {table_store}")
            # source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").append()   
            source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").using("iceberg").createOrReplace()
            # Verify the data transfer
            count_row = spark.sql(f"select count(*) as total_count from {table_store}").collect()[0]
            dest_df = count_row['total_count']  # or count_row.total_count
        else :
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{dest_db_name}.{dest_table_name}
            USING iceberg
            LOCATION 's3://{dest_bucket_name}/{dest_db_name}/{dest_table_name}'
            AS SELECT * FROM glue_catalog.{source_db_name}.{source_table_name} WHERE 1=0
            """)
            source_df = spark.read.format("iceberg").load(f"glue_catalog.{source_db_name}.{source_table_name}")


            source_count = source_df.count()
            logger.error(f"Source record count: {source_count}")
  
            # Write data to destination table
            source_df.writeTo(f"glue_catalog.{dest_db_name}.{dest_table_name}").append()   
            # Verify the data transfer
            dest_df = spark.read.format("iceberg").load(f"glue_catalog.{dest_db_name}.{dest_table_name}")
        print("Source table count:", source_df.count())
        print("Destination table count:", dest_df.count())
        
        # Optional: Show sample data from both tables
        print("\nSource table sample:")
        source_df.show(5)
        print("\nDestination table sample:")
        dest_df.show(5)
    except Exception as e:
        logger.error(f"Error during transfer: {str(e)}")
        raise

    

def main():

    # Source and destination S3 bucket configurations
    source_bucket = sys.argv[1]
    source_db=sys.argv[2]
    source_table = sys.argv[3] 
    dest_bucket = sys.argv[4]
    dest_db= sys.argv[5]
    dest_table = sys.argv[6]
    transfertype = sys.argv[7]

    logger.info(f"Transfer Configuration: {transfertype}")
    logger.info(f"Source: {source_bucket}/{source_db}/{source_table}")
    logger.info(f"Destination: {dest_bucket}/{dest_db}/{dest_table}")
        

    transfer_iceberg_table(
        dest_bucket,
        source_table,
        dest_table,
        source_db, 
        dest_db,
        transfertype
    )
    
    # Optional: Add table properties for optimization
    # spark.sql(f"""
    # ALTER TABLE glue_catalog.default.{dest_table} 
    # SET PROPERTIES (
    #     'write.format.default' = 'parquet',
    #     'write.parquet.compression-codec' = 'snappy',
    #     'write.metadata.compression-codec' = 'gzip'
    # )
    # """)

if __name__ == "__main__":
    main()
    spark.stop()
