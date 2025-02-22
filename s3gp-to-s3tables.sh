#! /bin/bash

#export AWS_REGION=cn-northwest-1
export AWS_REGION=us-east-1
export CLUSTER_NAME=eks-emr-example
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}
export S3tablebucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}
aws s3 mb $S3blogbucket --region $AWS_REGION
aws s3 cp resources/icebergtos3tables.py \
  ${S3blogbucket}/eks-emr-spark/icebergtos3tables.py
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

export S3blogbucket=s3://emr-eks-spark-${AWS_REGION}-${AWS_ACCOUNT_ID}
export source_db=default
export source_table=iceberg_table
export dest_bucket=my-s3-tablebucket0
export dest_db=mydemodb5
export dest_table=new_${source_table}
export transfertype=s3tables


aws emr-containers start-job-run \
--virtual-cluster-id $VIRTUAL_CLUSTER_ID \
--name myjob2-iceberg \
--execution-role-arn $EMR_ROLE_ARN \
--release-label emr-7.5.0-latest \
--region $AWS_REGION \
--job-driver '{
    "sparkSubmitJobDriver": {
        "entryPoint": "'${S3blogbucket}'/eks-emr-spark/icebergtos3tables.py",
        "entryPointArguments": ["'${S3blogbucket}'", "'${source_db}'", "'${source_table}'", "'${dest_bucket}'", "'${dest_db}'", "'${dest_table}'", "'${transfertype}'"],
        "sparkSubmitParameters": "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3 --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://emr-eks-spark-us-east-1-509399592849/example-prefix/ --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':bucket/'${dest_bucket}' --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
    }' \
--configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.driver.memory": "1G",          
          "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
          "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
          "spark.sql.catalog.glue_catalog.warehouse": "'${S3blogbucket}'/example-prefix/",
          "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
          "hive.metastore.glue.catalogid": "509399592849",
          "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
          "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-east-1:'${AWS_ACCOUNT_ID}':bucket/'${dest_bucket}'",
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
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