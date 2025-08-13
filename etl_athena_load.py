# Script to run athena query and store the results to Dataset.
# ParamStore  : SYSTEM.S3BUCKET.ATHENA, SYSTEM.S3BUCKET.LZ
# Job Args  : userid, domain_name, dataset_name, w_domain, w_dataset


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
athena_bucket_name = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.ATHENA", WithDecryption=False
)["Parameter"]["Value"]
lz_bucket_name = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False
)["Parameter"]["Value"]

# Athena query execution
athena_client = boto3.client("athena")

# Update below (or parameterize as job args)
userid = "harsha"
domain_name = "mississippi_gold"
w_domain = "demo"
w_dataset = "s3_athena_out"

# Athena query details
query = f"select * from mississippi_gold.immunization LIMIT 10;"
output_s3_path = f"s3://{athena_bucket_name}/{userid}"

# Start Athena query execution
response = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": domain_name},
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
    result_file_path = f"{output_s3_path}{query_execution_id}.csv"

    # Read the result into a Spark DataFrame
    athena_df = spark.read.csv(result_file_path, header=True)

    # Show the result
    athena_df.show()

else:
    print(f"Athena query failed with state: {state}")

upload_date = str(int(time.time()))
prefix = f"{w_domain}/{w_dataset}/upload_date={upload_date}/{userid}/csv/"
athena_df.write.csv(f"s3://{lz_bucket_name}/{prefix}", header=True)

# To reload Dataset use below. To Append comment below
# df_success = pd.Dataframe([])
# df_success.to_csv(f"s3://{lz_bucket_name}/{prefix}_SUCCESS")
