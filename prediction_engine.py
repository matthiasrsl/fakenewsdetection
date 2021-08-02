import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow
import numpy as np
from pyspark.ml import PipelineModel
import pandas as pd


@F.udf(returnType=StringType())
def format_output(prediction):
    return "FAKE NEWS" if prediction else "FACT"

spark = SparkSession.builder.appName("fakenewsdetection").getOrCreate()

model = PipelineModel.load("production_model_3")

tweets = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", "localhost:9092"
).option("subscribe", "test").load()

tweets_txt = tweets.selectExpr("CAST(value AS STRING)")

predictions = model.transform(tweets_txt).withColumn("value", format_output(F.col("prediction"))).select("value")

query = predictions.writeStream.format("kafka").option(
    "kafka.bootstrap.servers", "localhost:9092"
).option("topic", "test2").outputMode("append").option(
    "checkpointLocation", "./checkpoint"
).start()

time.sleep(25)

query.stop()