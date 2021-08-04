from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

@F.udf(returnType=ArrayType(StringType()))
def get_username(users, author_id):
    users_as_dicts = [row.asDict() for row in users]
    users_indexed = {user["id"]:(user["username"],user["name"]) for user in users_as_dicts}
    
    return users_indexed[author_id]

def get_preprocessing_writer(spark):
    """
    Builds and return the streaming writer to preprocess the tweets.
    Arguments:
    * spark: The SparkSession object
    """

    raw_data_schema = spark.read.json("raw_data_sample.json").schema

    raw_data = spark.readStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option(
        "subscribe", "raw_tweets"
    ).option(
        "failOnDataLoss", "false"
    ).load()

    structured_data = raw_data.selectExpr(
        "CAST(value AS STRING)"
    ).select(
        F.from_json(F.col("value"), raw_data_schema).alias("nested_value")
    ).select(
        "nested_value.*"
    )


    preprocessed_tweets = structured_data.select(
        F.col("includes.users"), "data.*"
    ).select(
        F.col("users").alias("users"), "text", F.col("id").alias("tweet_id"), "created_at", "author_id"
    ).withColumnRenamed(
        "id", "author_id"
    ).withColumn(
        "author_info", get_username(F.col("users"), F.col("author_id"))
    ).withColumn(
        "author_username", F.col("author_info").getItem(0)
    ).withColumn(
        "author_name", F.col("author_info").getItem(1)
    ).select(
        "tweet_id", "author_username", "author_name", "created_at", "text"
    )

    writer = preprocessed_tweets.selectExpr(
        "to_json(struct(*)) AS value"
    ).writeStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "preprocessed_tweets").outputMode("append").option(
        "checkpointLocation", "./checkpoint_preprocessing"
    )

    return writer
