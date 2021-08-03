import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import numpy as np
from pyspark.ml import PipelineModel
import pandas as pd


@F.udf(returnType=StringType())
def format_output(prediction):
    return "FAKE NEWS" if prediction else "FACT"

preprocessed_tweets_schema = StructType([
    StructField("tweet_id", StringType(), False),
    StructField("author_username", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("text", StringType(), False),
])

spark = SparkSession.builder.appName("fakenewsdetection").getOrCreate()

model = PipelineModel.load("production_model_3")

tweets = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", "localhost:9092"
).option(
    "subscribe", "preprocessed_tweets"
).option(
    "failOnDataLoss", "false"
).load()

tweets_txt = tweets.selectExpr(
    "CAST(value AS STRING)"
).select(
    F.from_json(F.col("value"), preprocessed_tweets_schema).alias("nested_value")
).select(
    "nested_value.*"
)

predictions = model.transform(tweets_txt).drop("words", "filtered", "features") # The intermediate columns created by the model are not intereting.

query = predictions.selectExpr(
    "to_json(struct(*)) AS value"
).writeStream.format("kafka").option(
    "kafka.bootstrap.servers", "localhost:9092"
).option("topic", "test2").outputMode("append").option(
    "checkpointLocation", "./checkpoint_prediction"
).start()

time.sleep(25)

query.stop()
