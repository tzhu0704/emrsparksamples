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


# Define JAR versions
ICEBERG_VERSION="1.8.0"
AWS_SDK_VERSION="2.18.230"
HADOOP_AWS_VERSION="3.3.4"

# Define the locations of required JARs
JARS_DIR="/data/software/emr/spark-3.5.4/jars"
ICEBERG_JAR="${JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
AWS_BUNDLE_JAR="${JARS_DIR}/bundle-${AWS_SDK_VERSION}.jar"
AWS_SDK_GLUE_JAR="${JARS_DIR}/aws-sdk-glue-${AWS_SDK_VERSION}.jar"
AWS_SDK_S3_JAR="${JARS_DIR}/aws-sdk-s3-${AWS_SDK_VERSION}.jar"
HADOOP_AWS_JAR="${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"


# Combine all JARs
ALL_JARS="${ICEBERG_JAR},${AWS_BUNDLE_JAR},${AWS_SDK_GLUE_JAR},${AWS_SDK_S3_JAR},${HADOOP_AWS_JAR}"
    
# Define the locations of required JARs
ICEBERG_JAR="/data/software/emr/spark-3.5.4/jars/iceberg-spark-runtime-3.5_2.12-1.8.0.jar"
AWS_BUNDLE_JAR="/data/software/emr/spark-3.5.4/jars/bundle-2.30.26.jar"

# Submit Spark job
spark-submit \
    --master local[2] \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
    --conf "spark.sql.catalog.glue_catalog.warehouse=${S3_WAREHOUSE}" \
    --conf "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
    --conf "spark.sql.defaultCatalog=glue_catalog" \
    --conf "spark.driver.memory=4g" \
    --conf "spark.executor.memory=4g" \
    --conf "spark.executor.instances=2" \
    ${S3_WAREHOUSE}


# Check if the job submission was successful
if [ $? -eq 0 ]; then
    echo "Spark job submitted successfully"
else
    echo "Failed to submit Spark job"
    exit 1
fi
