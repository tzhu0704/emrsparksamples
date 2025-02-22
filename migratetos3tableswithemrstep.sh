#! /bin/bash

# Check if required arguments are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_databases> <destination_bucket>"
    echo "  source_databases: Use comma-separated values for multiple databases or '*' for all databases"
    echo "  destination_bucket: S3 tables bucket name"
    exit 1
fi
export AWS_REGION=us-east-1

export CLUSTER_ID=$(aws emr list-clusters \
	--query "Clusters[?Status.State=='WAITING'].Id" \
	--region $AWS_REGION \
	--output text)

export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}
aws s3 mb $S3blogbucket --region $AWS_REGION
aws s3 cp resources/migratetos3tables.py \
  ${S3blogbucket}/eks-emr-spark/migratetos3tables.py

# Store command line arguments
export source_db="$1"
export dest_bucket="$2"

# Log the input parameters
echo "Migration Configuration:"
echo "----------------------"
echo "Source Database(s): $source_db"
echo "Destination Bucket: $dest_bucket"
echo "----------------------"
export CODE=${S3blogbucket}/eks-emr-spark/migratetos3tables.py
export JOB_NAME="Migration data from S3 general purpose to S3Tables"

aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"cluster\",\"--packages\",\"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3\",\"--conf\",\"spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog\",\"--conf\",\"spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog\",\"--conf\",\"spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO\",\"--conf\",\"spark.sql.catalog.glue_catalog.warehouse=$S3blogbucket/example-prefix/\",\"--conf\",\"spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog\",\"--conf\",\"spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog\",\"--conf\",\"spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-east-1:509399592849:bucket/$dest_bucket\",\"--conf\",\"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\",\"$CODE\",\"$source_db\",\"$dest_bucket\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"CONTINUE\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"MigrateToS3Tables\"}]"