# Fake News Detection

## Install
A Hadoop cluster with Spark 2.4.8 is required.

Make sure to place the following files on HDFS:
- `production_model_3/`
- `raw_data_sample.json`

## Commands (for development)

```
../kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic preprocessed_tweets

python3 twitter_producer.py

../spark248/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 --master yarn ./preprocessing_engine.py
```