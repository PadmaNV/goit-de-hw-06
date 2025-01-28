import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from configs import kafka_config, window_config

# Налаштування для Spark та Kafka
os.environ['SPARK_LOCAL_IP'] = '10.255.255.254'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Ініціалізація SparkSession
spark = (SparkSession.builder
         .appName("KafkaAlertsProcessor")
         .master("local[*]")
         .config("spark.sql.debug.maxToStringFields", "200")
         .config("spark.sql.columnNameLengthThreshold", "200")
         .getOrCreate())

# Завантаження параметрів алертів
alerts_conditions_path = "alerts_conditions.csv"
alerts_df = spark.read.csv(alerts_conditions_path, header=True, inferSchema=True)
print("Зчитані параметри алертів:")
alerts_df.show()

# Читання даних із Kafka-топіка VN_avg
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';")
      .option("subscribe", "VN_avg")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load())

# JSON-схема для обробки
json_schema = StructType([
    StructField("window", StructType([
        StructField("start", StringType(), True),
        StructField("end", StringType(), True)
    ]), True),
    StructField("t_avg", DoubleType(), True),
    StructField("h_avg", DoubleType(), True)
])

# Парсинг вхідних даних
df_parsed = (df.selectExpr("CAST(value AS STRING) AS value_deserialized")
              .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
              .select(
                  col("value_json.window.start").alias("window_start"),
                  col("value_json.window.end").alias("window_end"),
                  round(col("value_json.t_avg"), 1).alias("t_avg"),
                  round(col("value_json.h_avg"), 1).alias("h_avg"),
                  lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("timestamp")
              ))

# Виконання crossJoin з умовами алертів
all_alerts = df_parsed.crossJoin(alerts_df)

# Фільтрація алертів
valid_alerts = (all_alerts
                .where("t_avg >= temperature_min AND t_avg <= temperature_max")
                .unionAll(
                    all_alerts.where("h_avg >= humidity_min AND h_avg <= humidity_max")
                )
                .select("window_start", "window_end", "t_avg", "h_avg", "code", "message", "timestamp"))

# Вивід результату у консоль
query_console = (valid_alerts
         .writeStream
         .trigger(processingTime='30 seconds')
         .outputMode("update")
         .format("console")
         .start())

# Запис у Kafka-топік VN_avg_alerts
query_kafka = (valid_alerts
               .selectExpr("to_json(struct(*)) AS value")
               .writeStream
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
               .option("kafka.security.protocol", "SASL_PLAINTEXT")
               .option("kafka.sasl.mechanism", "PLAIN")
               .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_config['username']}' password='{kafka_config['password']}';")
               .option("topic", "VN_avg_alerts")
               .option("checkpointLocation", "/tmp/spark_alerts_checkpoint")
               .start())

# Запуск процесів
query_console.awaitTermination()
query_kafka.awaitTermination()
