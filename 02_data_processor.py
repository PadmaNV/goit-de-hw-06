import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from configs import kafka_config, window_config

# Налаштування для Spark та Kafka
os.environ['SPARK_LOCAL_IP'] = '10.255.255.254'
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Ініціалізація SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.sql.debug.maxToStringFields", "200")
         .config("spark.sql.columnNameLengthThreshold", "200")
         .getOrCreate())

# Схема JSON повідомлень з Kafka
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Виведення повідомлення про початок роботи
print("Starting to read data from Kafka topic VN_building_sensors...")

# Зчитування потоку даних із Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';")
      .option("subscribe", "VN_building_sensors")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", "100")
      .load())

# Обробка отриманих даних із Kafka
avg_stats = (df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*")
             .drop('key', 'value')
             .withColumnRenamed("key_deserialized", "key")
             .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
             .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"))
             .withWatermark("timestamp", window_config['watermark_duration'])
             .groupBy(window(col("timestamp"), window_config['window_duration'], window_config['sliding_interval']))
             .agg(
                 avg("value_json.temperature").alias("t_avg"),
                 avg("value_json.humidity").alias("h_avg")
             ))

# Виведення даних у консоль для перевірки
query_console = (avg_stats
         .writeStream
         .trigger(processingTime='30 seconds')
         .outputMode("update")
         .format("console")
         .start())

# Запис оброблених даних у Kafka-топік
query_kafka = (avg_stats
               .selectExpr("to_json(struct(*)) AS value")
               .writeStream
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
               .option("kafka.security.protocol", "SASL_PLAINTEXT")
               .option("kafka.sasl.mechanism", "PLAIN")
               .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';")
               .option("topic", "VN_avg")
               .option("checkpointLocation", "/tmp/spark_kafka_checkpoint")
               .start())

# Запуск обробки даних
query_console.awaitTermination()
query_kafka.awaitTermination()
