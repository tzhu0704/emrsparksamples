#! /bin/bash

# Check if required arguments are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_databases> <destination_bucket>"
    echo "  source_databases: Use comma-separated values for multiple databases or '*' for all databases"
    echo "  destination_bucket: S3 tables bucket name"
    exit 1
fi

export AWS_REGION=us-east-1
export CLUSTER_NAME=eks-emr-example
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}

aws s3 mb $S3blogbucket --region $AWS_REGION
aws s3 cp resources/migratetos3tables.py \
  ${S3blogbucket}/eks-emr-spark/migratetos3tables.py
aws s3 cp resources/spark_driver_podtemplate.yaml \
  ${S3blogbucket}/eks-emr-spark/spark_driver_podtemplate.yaml
aws s3 cp resources/spark_executor_podtemplate.yaml \
  ${S3blogbucket}/eks-emr-spark/spark_executor_podtemplate.yaml


export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters \
	--query "virtualClusters[?state=='RUNNING'].id" \
	--region $AWS_REGION \
	--output text)

export EMR_ROLE_ARN=$(aws iam get-role \
  --role-name EMRContainers-JobExecutionRole \
  --query Role.Arn \
  --region $AWS_REGION  \
  --output text)

# Store command line arguments
export source_db="$1"
export dest_bucket="$2"

# Log the input parameters
echo "Migration Configuration:"
echo "----------------------"
echo "Source Database(s): $source_db"
echo "Destination Bucket: $dest_bucket"
echo "----------------------"


aws emr-containers start-job-run \
--virtual-cluster-id $VIRTUAL_CLUSTER_ID \
--name migrate-iceberg-to-s3tables \
--execution-role-arn $EMR_ROLE_ARN \
--release-label emr-7.5.0-latest \
--region $AWS_REGION \
--job-driver '{
    "sparkSubmitJobDriver": {
        "entryPoint": "'${S3blogbucket}'/eks-emr-spark/migratetos3tables.py",
        "entryPointArguments": ["'${source_db}'", "'${dest_bucket}'"],
        "sparkSubmitParameters": "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3 --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse='${S3blogbucket}'/example-prefix/ --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':bucket/'${dest_bucket}' --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.driver.cores=2 --conf spark.driver.memory=4g --conf spark.executor.cores=4 --conf spark.executor.memory=4g"
        }
    }' \
--configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.driver.memory": "1G",          
          "spark.kubernetes.driver.podTemplateFile": "'${S3blogbucket}'/eks-emr-spark/spark_driver_podtemplate.yaml", 
          "spark.kubernetes.executor.podTemplateFile": "'${S3blogbucket}'/eks-emr-spark/spark_executor_podtemplate.yaml"
         }
      }
    ], 
    "monitoringConfiguration": {
      "cloudWatchMonitoringConfiguration": {
        "logGroupName": "/emr-on-eks/eksworkshop-eksctl", 
        "logStreamNamePrefix": "iceberg"
      }, 
      "s3MonitoringConfiguration": {
        "logUri": "'"$S3blogbucket"'/eks-emr-spark/logs/"
      }
    }
}'