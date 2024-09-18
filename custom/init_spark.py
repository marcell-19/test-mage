if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark.sql import SparkSession

@custom
def transform_custom(*args, **kwargs):
    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.access.key", 'superadmin') \
        .config("spark.hadoop.fs.s3a.secret.key", 'superadmin') \
        .config("spark.hadoop.fs.s3a.endpoint", 'http://minio:9000') \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    kwargs['context']['spark'] = spark


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
