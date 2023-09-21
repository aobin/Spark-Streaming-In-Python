from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("kafka Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # 设置 Kafka 相关配置
kafka_server = "hadoop1:9092"  # Kafka 服务器地址
kafka_topic = "my_topic"  # Kafka 主题名称

# 读取 Kafka 消息
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# 将 Kafka 消息内容提取出来并打印到终端
query = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 等待查询运行
query.awaitTermination()
