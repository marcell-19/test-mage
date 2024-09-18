if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
    
import os
from pyspark.sql.functions import col, hour, dayofweek, month, udf, year
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

categories = {
    'Dry': ['Dry', 'Almost Dry', 'Dry & Sunny', 'Dry Wet Road', 'Dry/wet', 'Dry & Wet', 'Dry A.m Wet P.m', 'Dry/sunny',
            'Dry/cloudy', 'Dry/hot', 'Very Hot Dry', 'Dry Dark', 'Dark Dry', 'Dry Mon', 'Dry Wed', 'Dry Thu', 'Dry Fri',
            'Dry Y', 'Good/dry', 'Dry/good'],
    'Wet': ['Wet', 'Slightly Wet', 'Wet Damp', 'Very Wet', 'V Wet', 'Wet - Dry', 'Dry - Wet', 'Wet/ Dry', 'S. Wet',
            'V. Wet', 'Wet Intermittently', 'Wet T', 'Wet & Windy', 'Wet & Very Windy', 'Wet Road', 'Wetish'],
    'Rain': ['Rain', 'Light Rain', 'Rain Stopped', 'Rain Damp', 'Rain Dry', 'Light Shower', 'Showers', 'Lt Rain',
             'Showers', 'Slight Drizzle', 'Very Heavy Rain', 'Rain/wind', 'Fine Drizzle', 'Rainy', 'Rain/dry',
             'Showers Mix', 'Rains', 'Sun/rain', 'Rain/dry', 'Occasional Lt Snow Shrs'],
    'Fine': ['Fine', 'Fine Windy', 'Fine (windy)', 'Fine Drizzle'],
    'Damp': ['Damp', 'Damp - Rain', 'Wet/dry', 'Damp - Rain'],
    'Cloudy': ['Cloudy', 'Cloudy/ Rain', 'Partly Cloudy', 'Sunny Cloudy'],
    'Foggy': ['Foggy'],
    'Drizzle': ['Drizzle', 'Slight Drizzle', 'Fine Drizzle'],
    'Showery': ['Showery', 'Light Showers', 'Heavy Showers'],
    'Other': ['Cold/rain', 'Cold/ Rain', 'Cold Windy Dry', 'Drty', 'Dry (windy)', 'Down Pour', 'Mist', 'Road Drying Sun Out',
              'Dark', 'Dark Sunny', 'Dark', 'Mild', 'Dryish', 'Light Shrs', 'Some Showers', 'Dry & Windy', 'Dry Windy',
              'Deluge', 'Dry & Very Windy', 'Windy', 'Windy/ Rain', 'Blustery', 'Cold', 'Down Pour', 'Hazy', 'Kdry',
              'Wet (windy)', 'Fine (windy)', 'Road Drying Sun Out', 'Wet (windy)', 'Dryish', 'Light Shrs', 'Some Showers',
              'Drizzle', 'Slight Drizzle', 'Fine Drizzle', 'Heavy Showers', 'Wet + Windy', 'Wet & Windy', 'Rain/wind',
              'Cold Windy Dry']
}

weather_to_category = {weather: category for category, weather_list in categories.items() for weather in weather_list}

# UDF to map the weather to its category
def map_weather(weather):
    return weather_to_category.get(weather, 'Other')  # Default to 'Other' if not found

def count_character_weather(weather):
    try:
        return len(weather)
    except:
        return 0

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    spark = kwargs['spark']

    # Register UDF
    map_weather_udf = udf(map_weather, StringType())
    count_weather_char_udf = udf(count_character_weather, IntegerType())

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .parquet(f's3a://cyclingetl/bronze/')

    # rename column to snake_case
    columns = df.columns

    for col in columns:
        df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

    # get hour
    df = df.withColumn("hour", hour("time")) \
        .withColumn("day_of_week", dayofweek("date")) \
        .withColumn("month", month("date")) \
        .withColumn("year_date", year("date")) \
        .withColumn("weather_category", map_weather_udf("weather")) \
        .withColumn("weather_count_char", count_weather_char_udf("weather")) \
        .filter(F.col("count") > 0)

    print(df.show(5))

    df.write.partitionBy("year_date", "month", "date").mode("append").parquet("s3a://cyclingetl/silver_new_banget_sih/")
    
    spark.stop()

    return 'OK'


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
