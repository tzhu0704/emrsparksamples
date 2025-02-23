#!/bin/bash


#s3://emr-eks-spark-us-east-1-509399592849/example-prefix
# Set environment variables
AWS_REGION="us-east-1"
S3_BUCKET="emr-eks-spark-us-east-1-509399592849"
S3_PREFIX="example-prefix"
S3_BUCKET="tzhubucket2"
S3_PREFIX="prefix1"
# LOCATION 's3://tzhubucket2/prefix1'
# Create the full S3 path
S3_WAREHOUSE="s3://${S3_BUCKET}/${S3_PREFIX}"
APP_FILE="resources/tpcds.py"
# Your Spark application jar/python file location
dest_bucket=mys3tablebucket10

# Define JAR versions
ICEBERG_VERSION="1.8.0"
AWS_SDK_VERSION="2.30.6"
HADOOP_AWS_VERSION="3.3.4"

# Define the locations of required JARs
JARS_DIR="/data/software/emr/spark-3.5.4/jars"
ICEBERG_JAR="${JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
AWS_BUNDLE_JAR="${JARS_DIR}/bundle-${AWS_SDK_VERSION}.jar"
AWS_SDK_GLUE_JAR="${JARS_DIR}/aws-sdk-glue-${AWS_SDK_VERSION}.jar"
AWS_SDK_S3_JAR="${JARS_DIR}/aws-sdk-s3-${AWS_SDK_VERSION}.jar"
HADOOP_AWS_JAR="${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"



# # Download required JARs if they don't exist
# if [ ! -f "${AWS_BUNDLE_JAR}" ]; then
#     echo "Downloading AWS bundle JAR..."
#     wget -O "${AWS_BUNDLE_JAR}" "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"
# fi

# if [ ! -f "${AWS_SDK_GLUE_JAR}" ]; then
#     echo "Downloading AWS SDK Glue JAR..."
#     wget -O "${AWS_SDK_GLUE_JAR}" "https://repo1.maven.org/maven2/software/amazon/awssdk/glue/${AWS_SDK_VERSION}/glue-${AWS_SDK_VERSION}.jar"
# fi

# if [ ! -f "${AWS_SDK_S3_JAR}" ]; then
#     echo "Downloading AWS SDK S3 JAR..."
#     wget -O "${AWS_SDK_S3_JAR}" "https://repo1.maven.org/maven2/software/amazon/awssdk/s3/${AWS_SDK_VERSION}/s3-${AWS_SDK_VERSION}.jar"
# fi

# if [ ! -f "${HADOOP_AWS_JAR}" ]; then
#     echo "Downloading Hadoop AWS JAR..."
#     wget -O "${HADOOP_AWS_JAR}" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
# fi

# Combine all JARs
ALL_JARS="${ICEBERG_JAR},${AWS_BUNDLE_JAR},${AWS_SDK_GLUE_JAR},${AWS_SDK_S3_JAR},${HADOOP_AWS_JAR}"
    
# Define the locations of required JARs
ICEBERG_JAR="/data/software/emr/spark-3.5.4/jars/iceberg-spark-runtime-3.5_2.12-1.8.0.jar"
AWS_BUNDLE_JAR="/data/software/emr/spark-3.5.4/jars/bundle-2.30.26.jar"



# Submit Spark job
# spark-submit \
#     --master local[2] \
#     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3 \
#     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
#     --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
#     --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
#     --conf spark.sql.catalog.glue_catalog.warehouse=${S3_WAREHOUSE} \
#     --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
#     --conf "spark.sql.defaultCatalog=glue_catalog" \
#     --conf "spark.driver.memory=4g" \
#     --conf "spark.executor.memory=4g" \
#     --conf "spark.executor.instances=2" \
#     --jars "${ALL_JARS}" \
#     ${APP_FILE} ${S3_WAREHOUSE}

spark-submit \
    --master local[2] \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,software.amazon.awssdk:s3:2.30.26 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue_catalog.warehouse=${S3_WAREHOUSE} \
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
    --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-east-1:509399592849:bucket/${dest_bucket} \
    --conf spark.sql.catalog.s3tablesbucket.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf "spark.driver.memory=4g" \
    --conf "spark.executor.memory=4g" \
    --conf "spark.executor.instances=2" \
    ${APP_FILE} ${S3_WAREHOUSE}


# Check if the job submission was successful
if [ $? -eq 0 ]; then
    echo "Spark job submitted successfully"
else
    echo "Failed to submit Spark job"
    exit 1
fi
