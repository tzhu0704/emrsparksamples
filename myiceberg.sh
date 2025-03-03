#! /bin/bash

#export AWS_REGION=cn-northwest-1
export AWS_REGION=us-east-1
export CLUSTER_NAME=eks-emr-example
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}

aws s3 mb $S3blogbucket --region $AWS_REGION
aws s3 cp resources/iceberg.py \
  ${S3blogbucket}/eks-emr-spark/iceberg.py
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

aws emr-containers start-job-run \
--virtual-cluster-id $VIRTUAL_CLUSTER_ID \
--name myjob2-iceberg \
--execution-role-arn $EMR_ROLE_ARN \
--release-label emr-6.15.0-latest \
--region $AWS_REGION \
--job-driver '{
    "sparkSubmitJobDriver": {
        "entryPoint": "'${S3blogbucket}'/eks-emr-spark/iceberg.py",
        "entryPointArguments": ["'${S3blogbucket}'"],
        "sparkSubmitParameters": "--jars local:///usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
        }
    }' \
--configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.driver.memory":"1G",
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
          "spark.sql.catalog.glue_catalog.warehouse": "'${S3blogbucket}'/example-prefix/",
          "spark.kubernetes.driver.podTemplateFile":"'${S3blogbucket}'/eks-emr-spark/spark_driver_podtemplate.yaml", 
          "spark.kubernetes.executor.podTemplateFile":"'${S3blogbucket}'/eks-emr-spark/spark_executor_podtemplate.yaml"
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