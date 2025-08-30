####################################################################################
# Description : Script to run athena query from S3 and store the results to Dataset..
# ParamStore  : SYSTEM.S3BUCKET.DLZ, SYSTEM.S3BUCKET.LZ, SYSTEM.S3BUCKET.ATHENA
# Dependencies : amorphicutils.zip
# GlueVersion : 5.0

# Job Arguments.
# --amorphic_job_id : Amorphic Glue Job ID
# --user_id : Amorphic UserId
# --output_dataset :  Output DatasetName. e.g. domain:dataset
# --sql_dataset :  DatasetName where SQL is stored. e.g. sql_domain:sql_dataset
# --read_sql_file_name : SQL file name to be read from S3. e.g. sample_data_query.sql
####################################################################################


import sys
import boto3
import time
import logging
import amorphicutils.awshelper as amorphic_helper
from awsglue.utils import getResolvedOptions
from amorphicutils.pyspark.infra.gluespark import GlueSpark
from amorphicutils.pyspark import write


logging.basicConfig(level='INFO')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Initialize GlueContext and SparkContext
glue_spark = GlueSpark()
glue_context = glue_spark.get_glue_context()
spark = glue_spark.get_spark()

athena_client = boto3.client("athena")

ssm_client = boto3.client("ssm")

ATHENA_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.ATHENA", WithDecryption=False
)["Parameter"]["Value"]
LZ_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False
)["Parameter"]["Value"]
DLZ_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.DLZ", WithDecryption=False
)["Parameter"]["Value"]

args = getResolvedOptions(sys.argv, ["JOB_NAME", "amorphic_job_id","user_id","output_dataset"])

RESOURCE_ID = args["amorphic_job_id"]
USERID = args["user_id"]
W_DOMAIN = args["output_dataset"].split(":")[0]
W_DATASET = args["output_dataset"].split(":")[1]
OUTPUT_S3_PATH = f"s3://{ATHENA_BUCKET_NAME}/glue-etl/{RESOURCE_ID}"


def read_sql_file(bucket_name,prefix, sql_file_name,region_name=None):
    """
    Reads a .sql file from S3 and returns the query as a string.
    :param bucket_name: Name of the S3 bucket. Defaults to 'us-east-1'.
    :param region: region of AWS S3 bucket.
    :param prefix: path of the SQL file in the bucket.
    :param sql_file_name: Name of the SQL file in the bucket.
    :return: SQL query as a string
    """
    
    LOGGER.info(
        "In read_sql_file, Bucket: %s, Prefix: %s, Filename %s",
        bucket_name,
        prefix,
        sql_file_name,
    )

    response_contents = amorphic_helper.list_bucket_objects(
        bucket_name=bucket_name, prefix=prefix, region=region_name
    )

    # print(response_contents)
    for content in response_contents:
        key = content["Key"]
        # filename pattern : <USERID>_<DATASET_ID>_<UPLOAD_EPOCH>_<FILENAME>
        filename = "_".join(key.split("/")[-1].split("_")[3:])
        if filename == sql_file_name:
            LOGGER.info("Reading the SQL file : %s", filename)
            s3_data = amorphic_helper.get_object(
                bucket_name=bucket_name, object_name=key
            )
            query = s3_data.read().decode("utf-8")
            return query
    
    LOGGER.info("File : %s not found. Check the file name.", sql_file_name)
    return None


def write_data(df, domain, dataset, user, full_reload=False, **kwargs):
    """
    Writes data to the Amorphic S3 using AmorphicUtils

    :param df: spark dataframe
    :param domain: Output Domain name for the dataset
    :param dataset: Output Dataset name
    :param user: Amorphic User ID with write access to the dataset.
    :return: Response dictionary containing exit codes,data and message.
    """
    csv_writer = write.Write(LZ_BUCKET_NAME,glue_context)
    response = csv_writer.write_csv_data(
        df,
        domain_name=domain,
        dataset_name=dataset,
        user=user,
        full_reload=full_reload,
        **kwargs
    )

    if response["exitcode"] == 0:
        LOGGER.info("Successfully written data to view dataset.")
    else:
        LOGGER.error(
            "Failed to write output to view dataset with error {error}".format(
                error=response["message"]
            )
        )
        raise Exception(response["message"])
    return response



def execute_athena_query(athena_query):
    """
    Execute Athena Query and return the result as Spark DataFrame
    :param QUERY: SQL Query to be executed in Athena
    :return: Spark DataFrame containing the result of the query
    """
    LOGGER.info("In execute_athena_query")
    LOGGER.debug("In execute_athena_query, Query: %s", athena_query)
    # Start Athena Query
    query_execution_id = start_query(athena_client, athena_query, OUTPUT_S3_PATH)

    # Wait for completion
    state, query_status = wait_for_query(athena_client, query_execution_id)

    if state == "SUCCEEDED":
        LOGGER.info(f"Athena query with id {query_execution_id} succeeded.")
        # Fetch the result file path
        result_file_path = f"{OUTPUT_S3_PATH}/{query_execution_id}.csv"
        # Read the result into a Spark DataFrame
        athena_df = spark.read.csv(result_file_path, header=True)
        return athena_df
    else:
        handle_failure(query_execution_id, state, query_status)

def start_query(athena_client, query, output_s3_path):
    """
    Start Athena Query Execution
    :param athena_client: Boto3 Athena Client
    :param query: SQL Query to be executed in Athena
    :param output_s3_path: S3 path where the query results will be stored
    :return: Query Execution ID
    """
    LOGGER.info("In start_query, Starting Athena Query Execution")
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": output_s3_path},
    )
    return response["QueryExecutionId"]

def wait_for_query(athena_client, query_execution_id):
    """
    Wait for Athena Query to complete
    :param athena_client: Boto3 Athena Client
    :param query_execution_id: Query Execution ID
    :return: Final state of the query and the query status response"""
    LOGGER.info("In wait_for_query, Waiting for Athena Query to complete")
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status["QueryExecution"]["Status"]["State"]
        if state not in ["QUEUED", "RUNNING"]:
            break
        time.sleep(1)  # Polling interval
    return state, query_status



def handle_failure(query_execution_id, state, query_status):
    """
    Handle Athena Query Failure
    :param query_execution_id: Query Execution ID
    :param state: Final state of the query
    :param query_status: Query status response
    :raise: Exception with the reason for failure"""
    LOGGER.error(f"Athena query with id {query_execution_id} failed with state: {state}")
    error_reason = query_status["QueryExecution"]["Status"]["StateChangeReason"]
    LOGGER.error(f"Reason for state {state}: {error_reason}")
    raise Exception(error_reason)


def main():
    """
    Main
    """
    LOGGER.info("In Main")
    
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "sql_dataset","read_sql_file_name"])
    sql_dataset = args["sql_dataset"]
    read_sql_file_name = args["read_sql_file_name"]
    print(f"SQL Dataset: {sql_dataset}, SQL File Name: {read_sql_file_name}")

    # in_domain:dataset -> in_domain/dataset/
    sql_dataset_prefix = f"{sql_dataset.strip().replace(':','/')}/" 
    print(f"SQL Dataset Prefix: {sql_dataset_prefix}")

    query = read_sql_file(DLZ_BUCKET_NAME, sql_dataset_prefix, read_sql_file_name)
    if query is not None:
        athena_spark_df = execute_athena_query(query) 
    
        if not athena_spark_df.rdd.isEmpty():
            # Show the result
            athena_spark_df.show()

            # Write the result to the output dataset
            response = write_data(
                athena_spark_df,
                domain=W_DOMAIN,
                dataset=W_DATASET,
                user=USERID,
                full_reload=False,
            )
            print("Write Response: ", response)
        else:
            print("No data returned from Athena query.")
    else:
        print("No query to execute. Exiting the job.")
        sys.exit(1)   

if __name__ == "__main__":
    main()