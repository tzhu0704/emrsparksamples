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
spark = (SparkSession.builder
         .config("spark.hadoop.hive.metastore.client.factory.class",
                 "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
         .enableHiveSupport()
         .getOrCreate())




def transfer_iceberg_table(dest_bucket_name, source_table_name, dest_table_name, source_db_name, dest_db_name,transfertype):
    try:
        # Read from source Iceberg table
        source_df = spark.read.format("iceberg").load(f"glue_catalog.{source_db_name}.{source_table_name}")
        
        if transfertype.lower() =='s3tables' :
            spark.sql(f"""create namespace if not exists {dest_db_name}""")
            # spark.sql(f"""show namespaces in demoblog""").show()
            # Create new Iceberg table in destination
            logger.info("Creating destination S3 table...")
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS s3tablesbucket.{dest_db_name}.{dest_table_name}
            USING iceberg
            AS SELECT * FROM glue_catalog.{source_db_name}.{source_table_name} WHERE 1=0
            """)
            # Write data to destination table
            source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").append()   
            # Verify the data transfer
            dest_df = spark.read.format("iceberg").load(f"s3tablesbucket.{dest_db_name}.{dest_table_name}")
        else :
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{dest_db_name}.{dest_table_name}
            USING iceberg
            LOCATION 's3://{dest_bucket_name}/{dest_db_name}/{dest_table_name}'
            AS SELECT * FROM glue_catalog.{source_db_name}.{source_table_name} WHERE 1=0
            """)
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

    logger.info("Transfer Configuration:")
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
