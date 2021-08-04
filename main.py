from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from preprocessing_engine import get_preprocessing_writer
from prediction_engine import get_prediction_writer
from twitter_producer import KafkaTwitterStream


if __name__ == "__main__":
    spark = SparkSession.builder.appName("fakenewsdetection").getOrCreate()

    stream = KafkaTwitterStream(
        "localhost:9092",
        "raw_tweets",
        bearer_token="AAAAAAAAAAAAAAAAAAAAANUlPgEAAAAA3n%2B61l0gpELzltUuU68CusMSFy4%3D6jmJKTf0VRzglbq4KgQQfNTGaUqAueQ06WtDUcoTLi4jiq4ajI"
    )

    preprocessing_writer = get_preprocessing_writer(spark)
    prediction_writer = get_prediction_writer(spark)

    preprocessing_query = preprocessing_writer.start()
    prediction_query = prediction_writer.start()

    try:
        stream.search_stream(
            tweet_fields=["text", "created_at"],
            expansions=["author_id"]
        )
    except KeyboardInterrupt:
        preprocessing_query.stop()
        prediction_query.stop()



