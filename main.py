import os
import importlib
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from dotenv import load_dotenv
import continuous_threading

from prediction_engine import get_prediction_writer
from postprocessing_engine import get_postprocessing_writer
from stream_producer import KafkaSourceStream


load_dotenv()
KAFKA_BROKER_IP = os.environ["KAFKA_BROKER_IP"]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("fakenewsdetection").getOrCreate()

    tree = os.listdir("producers")
    for filename in tree:
        module_name = filename.replace(".py", "")
        importlib.import_module("producers." + module_name)

    streams = []

    with open("producers.json", "r") as file:
        producers_params = json.load(file)["producers"]
    for params in producers_params:
        stream = KafkaSourceStream.producers_registry[params["name"]](
            KAFKA_BROKER_IP,
            "raw_documents",
            **params["kwargs"]
        )
        streams.append(stream)

    prediction_writer = get_prediction_writer(spark)
    postprocessing_writer = get_postprocessing_writer(spark)

    prediction_query = prediction_writer.start()
    postprocessing_query = postprocessing_writer.start()

    for stream in streams:
        stream.start_stream()
    
    try:
        while 1:
            pass
    except KeyboardInterrupt:
        for stream in streams:
            stream.stop_stream()

