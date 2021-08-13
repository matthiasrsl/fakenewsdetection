import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from preprocessing_engine import get_preprocessing_writer
from prediction_engine import get_prediction_writer
from postprocessing_engine import get_postprocessing_writer
from stream_producer import KafkaTwitterStream
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER_IP = os.environ["KAFKA_BROKER_IP"]
TWITTER_OAUTH_BEARER_TOKEN = os.environ["TWITTER_OAUTH_BEARER_TOKEN"]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("fakenewsdetection").getOrCreate()

    twitter_stream = KafkaTwitterStream(
        KAFKA_BROKER_IP,
        "raw_documents",
        bearer_token=TWITTER_OAUTH_BEARER_TOKEN
    )

    preprocessing_writer = get_preprocessing_writer(spark)
    prediction_writer = get_prediction_writer(spark)
    postprocessing_writer = get_postprocessing_writer(spark)

    #preprocessing_query = preprocessing_writer.start()
    prediction_query = prediction_writer.start()
    postprocessing_query = postprocessing_writer.start()

    #try:
    twitter_stream.start_stream()
    #except KeyboardInterrupt:
    #    preprocessing_query.stop()
    #    prediction_query.stop()
    #    postprocessing_query.stop()



