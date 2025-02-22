#!/bin/bash


#s3://emr-eks-spark-us-east-1-509399592849/example-prefix
# Set environment variables
AWS_REGION="us-east-1"
S3_BUCKET="emr-eks-spark-us-east-1-509399592849"
S3_PREFIX="example-prefix"

# Create the full S3 path
S3_WAREHOUSE="s3://${S3_BUCKET}/${S3_PREFIX}"

# Your Spark application jar/python file location
APP_FILE="resources/tpcds.py"

    

# Submit Spark job
spark-submit \
    --master local[2] \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
    --conf "spark.sql.catalog.glue_catalog.warehouse=${S3_WAREHOUSE}" \
    --conf "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
    --conf "spark.sql.defaultCatalog=glue_catalog" \
    --conf "spark.driver.memory=2g" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.executor.instances=2" \
    --jars /data/software/emr/spark-3.5.4/jars/iceberg-spark-runtime-3.5_2.12-1.8.0.jar \
    ${APP_FILE} ${S3_WAREHOUSE}

# Check if the job submission was successful
if [ $? -eq 0 ]; then
    echo "Spark job submitted successfully"
else
    echo "Failed to submit Spark job"
    exit 1
fi
