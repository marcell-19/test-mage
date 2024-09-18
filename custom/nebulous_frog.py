if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DecimalType, LongType

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    spark = kwargs['spark']

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .parquet(f's3a://cyclingetl/silver_new_banget_sih') \
        # .select("weather") \
        # .distinct()

    # temp_view = df.createOrReplaceTempView("tempSpark")

    # print(spark.sql("select * from tempSpark").take(5))

    # print(spark.sql("select count(*) from tempSpark").show())


    print(df.filter(df['weather'] == 'Rain Dry').show(10))

    # spark.stop()

    # data = [
    #     {
    #         "name": "Budi",
    #         "age": 5,
    #     }, {
    #         "name": "Doni",
    #         "age": 8
    #     }, {
    #         "name": "Roni",
    #         "age": 9
    #     }, 
    #     {
    #         "name": "fdsaf",
    #         "age": 9
    #     }
    # ]

    # schema = StructType([
    #     StructField("name", StringType(), True),
    #     StructField("age", IntegerType(), True)
    # ])

    # df = spark.createDataFrame(data, schema=schema)

    # alias
    # df = df.select(df.name.alias("first_name"))

    # distinct
    # df = df.distinct()

    # dropDuplicates
    # df = df.dropDuplicates(['age'])
    # simpleData = [("James","Sales","NY",90000,34,10000),
    #     ("Michael","Sales","NY",86000,56,20000),
    #     ("Robert","Sales","CA",81000,30,23000),
    #     ("Maria","Finance","CA",90000,24,23000),
    #     ("Raman","Finance","CA",99000,40,24000),
    #     ("Scott","Finance","NY",83000,36,19000),
    #     ("Jen","Finance","NY",79000,53,15000),
    #     ("Jeff","Marketing","CA",80000,25,18000),
    #     ("Kumar","Marketing","NY",91000,50,21000)
    # ]

    # schema = StructType([
    #     StructField("name", StringType(), True),
    #     StructField("department", StringType(), True),
    #     StructField("state", StringType(), True),
    #     StructField("salary", LongType(), True),
    #     StructField("age", IntegerType(), True),
    #     StructField("bonus", LongType(), True)
    # ])

    # df = spark.createDataFrame(data=simpleData, schema=schema)

    # # df = df.groupBy("department").sum("salary")
    # df = df.groupBy("department").min("salary")

    # print(df.show(5))
    # print(df.printSchema())


    # emp = [(1,"Smith",-1,"2018","10","M",3000), \
    #     (2,"Rose",1,"2010","20","M",4000), \
    #     (3,"Williams",1,"2010","10","M",1000), \
    #     (4,"Jones",2,"2005","10","F",2000), \
    #     (5,"Brown",2,"2010","40","",-1), \
    #     (6,"Brown",2,"2010","50","",-1) \
    # ]

    # empColumns = ["emp_id","name","superior_emp_id","year_joined", \
    #     "emp_dept_id","gender","salary"]

    # empDF = spark.createDataFrame(data=emp, schema = empColumns)
    # empDF.printSchema()
    # empDF.show(truncate=False)

    # dept = [("Finance",10), \
    #     ("Marketing",20), \
    #     ("Sales",30), \
    #     ("IT",40) \
    # ]

    # deptColumns = ["dept_name","dept_id"]
    # deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
    # deptDF.printSchema()
    # deptDF.show(truncate=False)

    # empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
    #     .show(truncate=False)

    spark.stop()

    return 'OK'


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
