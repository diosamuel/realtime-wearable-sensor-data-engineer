from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("wearable-sparksql")
    .getOrCreate()
)


print(spark)

# sudo docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/train.py
