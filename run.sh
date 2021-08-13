export SPARK_HOME=/home/huser/spark248

${SPARK_HOME}/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 --master yarn ./main.py