from pyspark.sql import SparkSession

# # Initialize Spark session
# spark = SparkSession.builder \
#     .master("spark://spark-master:7077") \
#     .appName("cobaspark") \
#     .getOrCreate()

# # Test the connection
# df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
# df.show()

# # Stop Spark session
# spark.stop()

from minio import Minio
from minio.error import S3Error

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

spark = SparkSession.builder \
    .appName("dfdddd") \
    .config("spark.jars", "/home/src/spark-jar/aws-java-sdk-bundle-1.11.1026.jar,/home/src/spark-jar/hadoop-aws-3.3.2.jar,/home/src/spark-jar/wildfly-openssl-1.0.7.Final.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", 'http://minio:9000') \
    .config("spark.hadoop.fs.s3a.access.key", "superadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "superadmin") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

try:
    # List all files in the specified bucket
    objects = client.list_objects(bucket_name, prefix=folder_prefix, recursive=True)

    for obj in objects:
        df = spark.read \
            .option("header", "true") \
            .csv(f's3a://{bucket_name}/{obj.object_name}')

        print(df.take(10))
except S3Error as e:
    print(f"An error occurred: {e}")

spark.stop()