from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

spark = SparkSession \
        .builder \
        .appName("File Streaming CSV") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()



userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv("/home/aobin/PycharmProjects/Spark-Streaming-In-Python/02-FileStreamDemo/SampleData/csv")
    
transformedDF = csvDF.selectExpr("*","age +1 as addAge")



csvData = transformedDF.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .option("path","/home/aobin/PycharmProjects/Spark-Streaming-In-Python/02-FileStreamDemo/SampleData/csv/output") \
        .start()

csvData.awaitTermination()

