if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark.sql import SparkSession
import os
from minio import Minio
from minio.error import S3Error
from pyspark.sql.functions import col, year, month, to_date, sum as _sum


@data_loader
def load_data(*args, **kwargs):
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 pyspark-shell'
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Initialize MinIO client
    client = Minio(
        "minio:9000",  # Replace with your MinIO server address
        access_key="superadmin",  # Replace with your access key
        secret_key="superadmin",  # Replace with your secret key
        secure=False  # Set to True if you're using HTTPS
    )

    # Specify the bucket name
    bucket_name = "cyclingetl"
    folder_prefix = "raw/"

    spark = kwargs['spark']

    try:
        # List all files in the specified bucket
        objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=True)

        for obj in objects:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f's3a://{bucket_name}/{obj.object_name}')

            df = df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))

            df = df.orderBy(col("Date"))

            df.write.mode("append").parquet("s3a://cyclingetl/bronze/")

            
    except S3Error as e:
        print(f"An error occurred: {e}")

    spark.stop()

    return "OK"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
