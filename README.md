# Fake News Detection

![Kibana screenshot](docs/screenshot_kibana_1.png)

## Install
A Hadoop cluster with Spark 2.4.8 and Python 3.6 is required.

```sh
$ sudo apt install software-properties-common  # To install command 'add-apt-repository'
$ sudo add-apt-repository ppa:deadsnakes/ppa  # To get python3.6 on Ubuntu 20.04
$ sudo apt update
$ sudo apt install python3.6
```

And edit ~/.bashrc to add:
```sh
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6
```

Make sure to place the following files on HDFS:
- `production_model_3/`
- `raw_data_sample.json`

```sh
$ cd models/
$ hdfs dfs -put production_model_3
$ cd ..
$ hdfs dfs -put raw_data_sample.json
```

## Running (WIP)
Starting ElasticSearch and Kibana if they are not running:
```
sudo systemctl start elasticsearch.service
sudo systemctl start kibana.service
```

Start Kafka Connect if not already running:
```
../kafka/bin/connect-distributed.sh ../kafka/config/connect-distributed.properties
```

Run the program:
```
../spark248/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 org.apache.spark:spark-avro:3.1.1 --master yarn ./main.py
```

## Commands (for development)

```
../kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic preprocessed_tweets

python3 twitter_producer.py

../spark248/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 --master yarn ./main.py
```


## Todo
- Remove environment variables from code
- Fix the streaming queries not closed properly
- Add the tweet id as key in Kafka records.
