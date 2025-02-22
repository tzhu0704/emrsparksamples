from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Iceberg Glue PySpark Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-bucket/warehouse/") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

    # 创建 Iceberg 表
    spark.sql("""
        CREATE TABLE glue_catalog.default.iceberg_table (
            id BIGINT,
            data STRING,
            ts TIMESTAMP
        ) USING iceberg
    """)

    # 插入数据
    spark.sql("""
        INSERT INTO glue_catalog.default.iceberg_table 
        VALUES 
            (1, 'test1', current_timestamp()), 
            (2, 'test2', current_timestamp())
    """)

    # 查询数据
    spark.sql("SELECT * FROM glue_catalog.default.iceberg_table").show()

    spark.stop()