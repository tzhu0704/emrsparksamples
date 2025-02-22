#! /bin/bash

#export AWS_REGION=cn-northwest-1
export AWS_REGION=us-east-1
export CLUSTER_NAME=eks-emr-example
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}

aws s3 mb $S3blogbucket --region $AWS_REGION
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
--name myjob3-glue-integration \
--execution-role-arn $EMR_ROLE_ARN \
--release-label emr-5.35.0-latest \
--region $AWS_REGION \
--job-driver '{
    "sparkSubmitJobDriver": {
        "entryPoint": "s3://aws-data-analytics-workshops/emr-eks-workshop/scripts/spark-etl-glue.py",
        "entryPointArguments": [
          "s3://aws-data-analytics-workshops/shared_datasets/tripdata/","'${S3blogbucket}'/taxi-data-glue/","tripdata"
        ],
        "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1"
        }
    }' \
--configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.hadoop.hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
         }
      }
    ], 
    "monitoringConfiguration": {
      "cloudWatchMonitoringConfiguration": {
        "logGroupName": "/emr-on-eks/eksworkshop-eksctl", 
        "logStreamNamePrefix": "glue"
      }, 
      "s3MonitoringConfiguration": {
        "logUri": "'${S3blogbucket}'/logs/"
      }
    }
}'