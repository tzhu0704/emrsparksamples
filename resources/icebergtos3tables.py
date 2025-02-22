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
spark.sparkContext.setLogLevel("ERROR")  # Options: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


def transfer_iceberg_table(source_db_name,source_table_name):
    try:
        # Read from source Iceberg table      
        logger.info(f"""create namespace if not exists s3tablesbucket.{source_db_name}""")
        spark.sql(f"""create namespace if not exists s3tablesbucket.{source_db_name}""")
        # Create new Iceberg table in destination
        logger.info(f"reading source S3 table...glue_catalog.{source_db_name}.{source_table_name}")
        
        source_df = spark.read.format("iceberg").load(f"glue_catalog.{source_db_name}.{source_table_name}")

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.{source_db_name}.{source_db_name}
        USING iceberg
        AS SELECT * FROM glue_catalog.{source_db_name}.{source_table_name} WHERE 1=0
        """)

        logger.info(f"Migragte to destination S3 table...{source_table_name}")     
        # Write data to destination table
        table_store = f"s3tablesbucket.{source_db_name}.{source_db_name}"
        logger.info(f"table_store: {table_store}")
        # source_df.writeTo(f"s3tablesbucket.{dest_db_name}.{dest_table_name}").append()   
        source_df.writeTo(f"s3tablesbucket.{source_db_name}.{source_db_name}").using("iceberg").createOrReplace()
        # Verify the data transfer
        count_row = spark.sql(f"select count(*) as total_count from {table_store}").collect()[0]
        dest_df = count_row['total_count']  # or count_row.total_count
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


def transfer_iceberg_db(source_db_name):
    try:
        # Read from source Iceberg table
       
        logger.info(f" list all databases in glue_catalog ")
        spark.sql(f"list all databases in glue_catalog")

        databases_df = spark.sql("SHOW DATABASES in glue_catalog")
        databases = [row.databaseName for row in databases_df.collect()]
        
        logger.info(f"Found {len(databases)} databases")
        
        #Dictionary to store database and table information
        catalog_info = {}

        # Iterate through each database
        for db in databases:
            try:
                logger.info(f"Scanning database: {db}")
         
  
                # Get tables for current database
                tables_df = spark.sql(f"SHOW TABLES IN glue_catalog.{db}")
                tables = [(row.tableName, row.isTemporary) for row in tables_df.collect()]
                
                # Store in dictionary
                catalog_info[db] = tables
                
                logger.info(f"Database {db}: Found {len(tables)} tables")
                
                # Print detailed table information
                for table_name, is_temp in tables:
                    try:
                        # Get table details
                        table_details = spark.sql(f"DESCRIBE TABLE EXTENDED {db}.{table_name}").collect()
                        
                        # Get row count (wrapped in try-catch as it might fail for some tables)
                        try:
                            count_row = spark.sql(f"SELECT COUNT(*) as count FROM {db}.{table_name}").collect()[0]
                            row_count = count_row['count']
                        except Exception as count_error:
                            logger.warning(f"Could not get row count for {db}.{table_name}: {str(count_error)}")
                            row_count = "N/A"

                        logger.info(f"""
                        Table: {db}.{table_name}
                        Temporary: {is_temp}
                        Row Count: {row_count}
                        """)
                        
                    except Exception as table_error:
                        logger.error(f"Error getting details for table {db}.{table_name}: {str(table_error)}")
                        continue

            except Exception as db_error:
                logger.error(f"Error processing database {db}: {str(db_error)}")
                continue

        return catalog_info

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
        logger.info(f"dest_df...{dest_df}") 
            
        
        print("Source table count:", source_df.count())
        print("Destination table count:", dest_df.count())
        
        # Optional: Show sample data from both tables
        # print("\nSource table sample:")
        # source_df.show(5)
        # print("\nDestination table sample:")
        # dest_df.show(5)
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
