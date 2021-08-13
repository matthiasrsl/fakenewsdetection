import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

import numpy as np
from pyspark.ml import PipelineModel

import numpy as np


@F.udf(returnType=StringType())
def format_output(prediction):
    return "FAKE" if prediction else "REAL"

preprocessed_tweets_schema = StructType([
    StructField("tweet_id", StringType(), False),
    StructField("author_username", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("text", StringType(), False),
])

def get_prediction_writer(spark):
    """
    Builds and return the streaming writer to preprocess the tweets.
    Arguments:
    * spark: The SparkSession object
    """

    model = PipelineModel.load("production_model_3")

    tweets = spark.readStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option(
        "subscribe", "raw_tweets"
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

    predictions = model.transform(tweets_txt)

    predictions_formatted = predictions.withColumn(
        "class", format_output(F.col("prediction"))
    ).drop(  # The intermediate columns created by the model are not intereting.
        "words", "filtered", "features", "rawPrediction" 
    )

    writer = predictions_formatted.selectExpr(
        "to_json(struct(*)) AS value"
    ).writeStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "predictions").outputMode("append").option(
        "checkpointLocation", "./checkpoint_prediction"
    )

    return writer
