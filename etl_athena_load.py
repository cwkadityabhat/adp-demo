##########################################################################
# Description : Script to run athena query and store the results to Dataset..
# ParamStore  : SYSTEM.S3BUCKET.LZ, SYSTEM.S3BUCKET.ATHENA
# Dependencies : None
# GlueVersion : 5.0

# Update below variables in the script.
# RESOURCE_ID : Amorphic Glue Job ID
# USERID : Amorphic UserId
# W_DOMAIN :  Output Domain
# W_DATASET :  Output DatasetName
# QUERY : Athena SQL Query.
##########################################################################



import sys
import boto3
import time
import pandas as pd
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


# Initialize GlueContext and SparkContext
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

ssm_client = boto3.client("ssm")
ATHENA_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.ATHENA", WithDecryption=False
)["Parameter"]["Value"]
LZ_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False
)["Parameter"]["Value"]

# Athena query execution
athena_client = boto3.client("athena")

# Update below (or parameterize as job args)
RESOURCE_ID = "amorphic_job_id" # Job Id
USERID = "userid"
W_DOMAIN = "output_domain"
W_DATASET = "output_dataset""

# Athena query details
QUERY = f"select * from domain.dataset LIMIT 10;"
output_s3_path = f"s3://{ATHENA_BUCKET_NAME}/glue-etl/{RESOURCE_ID}"

# Start Athena query execution
response = athena_client.start_query_execution(
    QueryString=QUERY,
    ResultConfiguration={"OutputLocation": output_s3_path},
)

# Get query execution ID
query_execution_id = response["QueryExecutionId"]

# Wait for the query to complete
query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
state = query_status["QueryExecution"]["Status"]["State"]

# Wait for query to complete
while state in ["QUEUED", "RUNNING"]:
    query_status = athena_client.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    state = query_status["QueryExecution"]["Status"]["State"]

# If the query succeeded, read the result from the S3 output location
if state == "SUCCEEDED":
    # Fetch the result file path
    result_file_path = f"{output_s3_path}/{query_execution_id}.csv"

    # Read the result into a Spark DataFrame
    athena_df = spark.read.csv(result_file_path, header=True)

    # Show the result
    athena_df.show()

    # Write to Dataset
    upload_date = str(int(time.time()))
    prefix = f"{W_DOMAIN}/{W_DATASET}/upload_date={upload_date}/{USERID}/csv/"
    athena_df.write.csv(f"s3://{LZ_BUCKET_NAME}/{prefix}", header=True)

    # To reload Dataset use below. To Append uncomment below
    # df_success = pd.DataFrame([])
    # df_success.to_csv(f"s3://{lz_bucket_name}/{prefix}_SUCCESS")

else:
    print(f"Athena query with id {query_execution_id} failed with state: {state}")
    error_reason = query_status['QueryExecution']['Status']['StateChangeReason']
    print(f"Reason for state {state} : {error_reason}")
    raise Exception(error_reason)

