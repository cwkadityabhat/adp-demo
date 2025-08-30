##########################################################################
# Description : Script to run Spark query and store the results to Dataset.
# ParamStore  : SYSTEM.S3BUCKET.LZ, SYSTEM.S3BUCKET.DLZ
# Dependencies : amorphicutils.zip
# GlueVersion : 4.0

# Update below variables in the script.
# USERID : Amorphic UserId
# W_DOMAIN :  Output Domain
# W_DATASET :  Output DatasetName
# INPUT_DATASET_MAPPING : 
#           Dictionary of input Dataset mapping. 
#           The keys are your table alias in your query (ex: dataset_name_1)
# QUERY : Spark SQL Query. Note: Replace actual table names with alias

##########################################################################


import sys
import boto3
import time
import logging
import pandas as pd
from awsglue.utils import getResolvedOptions
from amorphicutils.pyspark.infra.gluespark import GlueSpark
from amorphicutils.pyspark import read
from amorphicutils.pyspark import write

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Initialize GlueContext and SparkContext
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

glue_spark = GlueSpark()
glue_context = glue_spark.get_glue_context()
spark = glue_spark.get_spark()

ssm_client = boto3.client("ssm")
lz_bucket_name = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False
)["Parameter"]["Value"]
dlz_bucket_name = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.DLZ", WithDecryption=False
)["Parameter"]["Value"]


# Update below (or parameterize as job args)
USERID = "userid"
W_DOMAIN = "output_domain"
W_DATASET = "output_dataset"

# Update below table mapping used in your query. DatasetNames are case-sensitive
INPUT_DATASET_MAPPING = {
    "dataset_name_1": {
        "domain": "domain_1", 
        "dataset": "DatasetName1",
    },
    "dataset_name_2": {
        "domain": "domain_2",
        "dataset": "DatasetName2",
    },
}
# Define the SQL query
QUERY = """
    SELECT * FROM dataset_name_1
    UNION ALL
    SELECT * FROM dataset_name_2
"""

def read_data(input_mapping):
    """
    Read function to read all the input datasets used in view.

    It iterates through dictionary of input_dataset_mapping and read
    the datasets from S3 using the read_csv_data function.

    :param input_dataset_mapping: Dictionary containing dataset information.
    :return:
    """
    df_dict = {}
    csv_reader = read.Read(dlz_bucket_name, spark=spark)
    for dataset_name_key, dataset_info in input_mapping.items():
        LOGGER.info(
            "Reading dataset {name} from S3".format(name=dataset_info["dataset"])
        )
        # Read the dataset from S3
        domain_name = dataset_info["domain"]
        dataset_name = dataset_info["dataset"]

        df_dict[dataset_name_key] = csv_reader.read_csv_data(
            domain_name=domain_name,
            dataset_name=dataset_name,
            header=True,
            schema=None,
            escape='"',
        )
        if df_dict[dataset_name_key]["exitcode"] != 0:
            LOGGER.error(
                "Failed to read dataset with error {error}".format(
                    error=df_dict[dataset_name_key]["message"]
                )
            )
            raise Exception(df_dict[dataset_name_key]["message"])
    return df_dict


def write_data(df, domain, dataset, user, full_reload=False, **kwargs):
    """
    Writes data to the Amorphic S3 using AmorphicUtils

    :param df: spark dataframe
    :param domain: Output Domain name for the dataset
    :param dataset: Output Dataset name
    :param user: Amorphic User ID with write access to the dataset.
    :return: Response dictionary containing exit codes,data and message.
    """
    epoch = str(int(time.time()))
    csv_writer = write.Write(lz_bucket_name, glue_context)
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


def create_view(inp_df_dict,query):
    """
    Create a view from the input datasets.
    :param inp_df_dict: Dictionary containing spark input dataframe.
    :param query: SQL query to create the view.
    :return: Spark DataFrame containing the result of the query.
    """
    for dataset_name_key, dataset_data in inp_df_dict.items():
        dataset_data["data"].createOrReplaceTempView(dataset_name_key)
    spark_df = spark.sql(query)
    return spark_df


def main():
    """
    Main function to execute the script.
    """
    LOGGER.info("Creating spark from Input Mapping")

    input_df_dict = read_data(INPUT_DATASET_MAPPING)

    # Create the spark df using the SQL query
    LOGGER.info("Creating spark df with query: {}".format(QUERY))
    spark_df = create_view(input_df_dict,query=QUERY)
    spark_df.show(20, truncate=False)

    # Write the result to the output dataset
    response = write_data(
        spark_df,
        domain=W_DOMAIN,
        dataset=W_DATASET,
        user=USERID,
        full_reload=False,
    )
    print("Write Response: ", response)


if __name__ == "__main__":
    main()
