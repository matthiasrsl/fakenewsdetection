export SPARK_HOME=/home/huser/spark248
export KAFKA_HOME=/home/huser/kafka

#${KAFKA_HOME}/bin/connect-distributed.sh ${KAFKA_HOME}/config/connect-distributed.properties &
${SPARK_HOME}/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 --master yarn ./main.py