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

def get_catalog_databases(spark):
    """Retrieve all databases from glue catalog"""
    try:
        logger.info("Retrieving databases from glue_catalog")
        databases_df = spark.sql("SHOW DATABASES in glue_catalog")
        databases = [row.namespace for row in databases_df.collect()]
        logger.info(f"Found {len(databases)} databases in glue_catalog")
        return databases
    except Exception as e:
        logger.error(f"Error retrieving databases: {str(e)}")
        raise

def get_database_tables(spark, database):
    """Retrieve all tables from a specific database"""
    try:
        logger.info(f"Retrieving tables from database: {database}")
        tables_df = spark.sql(f"SHOW TABLES IN glue_catalog.{database}")
        tables = [row.tableName for row in tables_df.collect()]
        logger.info(f"Found {len(tables)} tables in {database}")
        return tables
    except Exception as e:
        logger.error(f"Error retrieving tables for database {database}: {str(e)}")
        raise

def transfer_table(spark, source_db, table_name, dest_bucket):
    """Transfer a single table"""
    try:
        logger.info(f"Transferring table: {source_db}.{table_name}")
        sys.stdout.write(f"Starting transfer of {source_db}.{table_name}\n")
        sys.stdout.flush()

        # Read source table
        source_df = spark.read.format("iceberg").load(f"glue_catalog.{source_db}.{table_name}")
        
        # Get source count
        source_count = source_df.count()
        logger.info(f"Source table {table_name} has {source_count} records")

        # Create destination table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket.{source_db}.{table_name} 
        USING iceberg
        AS SELECT * FROM glue_catalog.{source_db}.{table_name} WHERE 1=0
        """

        logger.info(f"create_table_sql--{create_table_sql}")

        spark.sql(create_table_sql)

        # Write data
        logger.info(f"Writing data to s3tablesbucket.{source_db}.{table_name}")
        source_df.writeTo(f"s3tablesbucket.{source_db}.{table_name}").using("iceberg").createOrReplace()

        # Verify count
        dest_count = spark.sql(f"SELECT COUNT(*) as cnt FROM s3tablesbucket.{source_db}.{table_name}").collect()[0]['cnt']
        
        if source_count != dest_count:
            raise ValueError(f"Count mismatch for {table_name}! Source: {source_count}, Destination: {dest_count}")
        
        logger.info(f"Successfully transferred {table_name} with {dest_count} records")
        return True

    except Exception as e:
        logger.error(f"Error transferring table {table_name}: {str(e)}")
        return False

def process_database_migration(spark, source_db, dest_bucket):
    """Process migration for a single database"""
    try:
        logger.info(f"Processing database: {source_db}")
        sys.stdout.write(f"\nStarting migration for database: {source_db}\n")
        sys.stdout.flush()

        # Create namespace in destination if it doesn't exist
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{source_db}")
        
        # Get all tables in the database
        tables = get_database_tables(spark, source_db)
        
        # Track migration results
        results = {
            "successful": [],
            "failed": []
        }

        total_tables = len(tables)
        for index, table in enumerate(tables, 1):
            try:
                logger.info(f"Processing table {index}/{total_tables}: {table}")
                success = transfer_table(spark, source_db, table, dest_bucket)
                
                if success:
                    results["successful"].append(table)
                else:
                    results["failed"].append(table)
                
                # Show progress
                progress = (index / total_tables) * 100
                sys.stdout.write(f"Progress: {progress:.2f}% ({index}/{total_tables} tables)\n")
                sys.stdout.flush()

            except Exception as e:
                logger.error(f"Error processing table {table}: {str(e)}")
                results["failed"].append(table)

        return results

    except Exception as e:
        logger.error(f"Error processing database {source_db}: {str(e)}")
        raise

def main():
    try:
        if len(sys.argv) < 3:
            logger.error("Incorrect number of arguments")
            logger.info("Usage: script.py <source_databases> <dest_bucket>")
            sys.exit(1)

        source_databases = sys.argv[1]  # Comma-separated list of databases
        dest_bucket = sys.argv[2]
        
        # Get all available databases from glue catalog
        available_databases = get_catalog_databases(spark)
        logger.info(f"Available databases in glue catalog: {available_databases}")

        # Determine which databases to process
        if source_databases == '*':
            source_db_list = available_databases
            logger.info("Wildcard '*' specified - will process all available databases")
        else:
            # Split source databases
            source_db_list = [db.strip() for db in source_databases.split(',')]
            logger.info(f"Processing specified databases: {source_db_list}")


        # Get all available databases from glue catalog
        available_databases = get_catalog_databases(spark)

        # Track overall migration results
        migration_results = {}

        # Process each source database
        for db in source_db_list:
            if db in available_databases :
                logger.info(f"\nProcessing database: {db}")
                try:
                    # Create start time marker
                    start_time = datetime.now()
                    
                    # Process the database
                    results = process_database_migration(spark, db, dest_bucket)
                    
                    # Calculate duration
                    duration = datetime.now() - start_time
                    
                    # Store results
                    migration_results[db] = {
                        "status": "completed",
                        "successful_tables": results["successful"],
                        "failed_tables": results["failed"],
                        "duration": duration,
                        "total_tables": len(results["successful"]) + len(results["failed"])
                    }

                except Exception as e:
                    logger.error(f"Failed to process database {db}: {str(e)}")
                    migration_results[db] = {
                        "status": "failed",
                        "error": str(e)
                    }
            else:
                logger.warning(f"Database {db} not found in glue catalog. Skipping...")
                migration_results[db] = {
                    "status": "skipped",
                    "reason": "database not found"
                }

        # Print summary
        logger.info("\n=== Migration Summary ===")
        for db, results in migration_results.items():
            logger.info(f"\nDatabase: {db}")
            logger.info(f"Status: {results['status']}")
            
            if results['status'] == 'completed':
                logger.info(f"Total tables: {results['total_tables']}")
                logger.info(f"Successful: {len(results['successful_tables'])}")
                logger.info(f"Failed: {len(results['failed_tables'])}")
                logger.info(f"Duration: {results['duration']}")
                
                if results['failed_tables']:
                    logger.info("Failed tables:")
                    for table in results['failed_tables']:
                        logger.info(f"  - {table}")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Initialize Spark Session
    spark = (SparkSession.builder
            .config("spark.hadoop.hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
            .enableHiveSupport()
            .getOrCreate())

    try:
        main()
    finally:
        spark.stop()