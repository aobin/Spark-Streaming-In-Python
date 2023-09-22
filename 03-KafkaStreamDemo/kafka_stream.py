from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("KafkaReceive") \
        .getOrCreate()

    # 定义Kafka数据源
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "hadoop1:9092") \
      .option("subscribe", "my_topic") \
      .load()

    # 只选择"value"字段并将其转换为字符串
    df = df.selectExpr("CAST(value AS STRING)")

    # 写入控制台
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()