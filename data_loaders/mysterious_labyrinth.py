if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark.sql import SparkSession

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    spark = SparkSession.builder \
        .appName("halocuk") \
        .config("spark.jars", "/home/src/spark-jar/hadoop-aws-3.3.2.jar,/home/src/spark-jar/aws-java-sdk-bundle-1.11.1026.jar,/home/src/spark-jar/wildfly-openssl-1.0.7.Final.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", 'http://minio:9000') \
        .config("spark.hadoop.fs.s3a.access.key", "superadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "superadmin") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv('s3a://cyclingetl/raw/2014%20Q1%20(Jan-Mar)-Central.csv')

    df.show()

    spark.stop()

    return "OK"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
