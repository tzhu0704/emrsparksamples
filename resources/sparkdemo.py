from pyspark.sql import SparkSession
# JDK 11
spark = SparkSession.builder \
    .appName("Iceberg Example") \
    .config("spark.driver.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.executor.extraJavaOptions", "--illegal-access=permit") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,"
            "software.amazon.awssdk:s3:2.20.0,"
            "software.amazon.awssdk:sts:2.20.0,"
            "software.amazon.awssdk:kms:2.20.0,"
            "software.amazon.awssdk:glue:2.20.0,"
            "software.amazon.awssdk:dynamodb:2.20.0,"
            "software.amazon.awssdk:s3tables:2.29.26") \
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
    .config("spark.sql.catalog.s3tablesbucket.warehouse", "arn:aws:s3tables:us-east-1:509399592849:bucket/mys3tablebucket10") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
# # 创建命名空间
# spark.sql(" CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.example_namespace")
# # 创建 Iceberg 表
# spark.sql("CREATE TABLE s3tablesbucket.example_namespace.test_spark3 (id INT, data STRING) USING iceberg")
# #
# # 插入数据
# spark.sql("""
# INSERT INTO s3tablesbucket.example_namespace.test_spark3 VALUES (1, 'a'), (2, 'b'), (3, 'c')
# """)

# 查询数据
spark.sql("""SELECT * FROM s3tablesbucket.icebergdb1.iceberg_table1""").show()
#spark.sql("SELECT * FROM base1.create_demo_table1")
#spark.sql("""ALTER TABLE s3tablesbucket.testdb.test_table SET IDENTIFIER FIELDS id""")