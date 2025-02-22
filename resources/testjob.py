from pyspark.sql import SparkSession
import sys
from datetime import datetime
# import time
from time import sleep

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Initialize Spark Session with Iceberg support
spark = (SparkSession.builder
         .config("spark.hadoop.hive.metastore.client.factory.class",
                 "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")  # Options: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


def transfer_iceberg_table(dest_bucket_name, source_table_name, dest_table_name, source_db_name, dest_db_name,transfertype):
    try:
        # Read from source Iceberg table
        if transfertype.lower() =='s3tables' :
            logger.error(f"""create namespace if not exists s3tablesbucket.{dest_db_name}""")
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

            logger.error(f"Created destination S3 table...{transfertype}") 

            source_count = source_df.count()
            logger.error(f"Source record count: {source_count}")
             # Write data to destination table
            table_store = f"s3tablesbucket.{dest_db_name}.{dest_table_name}"
            logger.error(f"table_store: {table_store}")
            # source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").append()   
            source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").using("iceberg").createOrReplace()
            # Verify the data transfer
            logger.error(f"select count(*) as total_count from {table_store}")
            count_row = spark.sql(f"select count(*) as total_count from {table_store}").collect()[0]
            dest_df = count_row['total_count']  # or count_row.total_count
            logger.error(f"dest_df---- {dest_df}")

        print("\nDestination table sample:")
        # spark.stop()
    except Exception as e:
        logger.error(f"Error during transfer: {str(e)}")
        raise
    logger.info("this is a debug information.............")


    
def main():
    
    logger.error(f"Transfer Configuration------------------------")
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
    


if __name__ == "__main__":
    main()
    spark.stop()

