from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


postprocessed_predictions_schema = StructType([
    StructField("source", StringType(), False),
    StructField("type", StringType(), False),
    StructField("author_id", StringType(), False),
    StructField("author_name", StringType(), False),
    StructField("author_username", StringType(), False),
    StructField("class", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("prediction", DoubleType(), False),
    StructField("probability", StructType([
        StructField("type", LongType(), False),
        StructField("values", ArrayType(DoubleType()), False),
    ]), False),
    StructField("text", StringType(), False),
    StructField("document_id", StringType(), False),
    StructField("location", ArrayType(DoubleType()), True),
])


def get_postprocessing_writer(spark):
    """
    Builds and return the streaming writer to postprocess the prediction results.
    Arguments:
    * spark: The SparkSession object
    """

    raw_data = spark.readStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option(
        "subscribe", "predictions"
    ).option(
        "failOnDataLoss", "false"
    ).load()

    tweets_with_predictions = raw_data.selectExpr(
        "CAST(value AS STRING)"
    ).select(
        F.from_json(F.col("value"), postprocessed_predictions_schema).alias("nested_value")
    ).select(
        "nested_value.*"
    )


    postprocessed_tweets_with_predictions = tweets_with_predictions.withColumn(
        "prob", F.col("probability.values").getItem(1)
    ).drop("probability").withColumnRenamed("prob", "probability")

    writer = postprocessed_tweets_with_predictions.selectExpr(
        "to_json(struct(*)) AS value"
    ).writeStream.format("kafka").option(
        "kafka.bootstrap.servers", "localhost:9092"
    ).option("topic", "postprocessed_predictions").outputMode("append").option(
        "checkpointLocation", "./checkpoint_postprocessing"
    )

    return writer
