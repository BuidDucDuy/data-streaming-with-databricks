# databricks_notebook/kinesis_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, MapType

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("KinesisConsumer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# =========================================
# 1. ĐỊNH NGHĨA SCHEMA CỦA DỮ LIỆU ĐẦU VÀO
#    Phải khớp với cấu trúc JSON từ OpenWeatherMap + timestamp
# ==========================================
# Đây là schema rút gọn dựa trên API OpenWeatherMap
schema = StructType([
    StructField("coord", MapType(StringType(), DoubleType()), True),
    StructField("weather", StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True)
    ]), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("wind", MapType(StringType(), DoubleType()), True),
    StructField("name", StringType(), True), # Tên thành phố (ví dụ: Hanoi)
    StructField("cod", IntegerType(), True),
    StructField("timestamp", LongType(), True) # Timestamp chúng ta thêm vào
])

# ==========================================
# 2. ĐỌC DỮ LIỆU TỪ KINESIS STREAM
# ==========================================
kinesis_stream_name = "realtime-api-stream" # Tên Kinesis stream của bạn
aws_region = "ap-southeast-1" # Region AWS của bạn (phải khớp với Kinesis)

# Đảm bảo cluster Databricks có quyền IAM để đọc từ Kinesis.
# (kinesis:GetRecords, kinesis:GetShardIterator, kinesis:DescribeStream, kinesis:ListShards)

df_kinesis = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("region", aws_region) \
    .option("initialPosition", "LATEST") \
    .load()


# ==========================================
# 3. XỬ LÝ DỮ LIỆU
# ==========================================
df_parsed = df_kinesis \
    .selectExpr("CAST(data AS STRING) as json_data") \
    .withColumn("parsed_data", from_json(col("json_data"), schema)) \
    .select(
        col("parsed_data.name").alias("city_name"),
        col("parsed_data.weather.main").alias("weather_condition"),
        col("parsed_data.weather.description").alias("weather_description"),
        col("parsed_data.main.temp").alias("temperature_celsius"),
        col("parsed_data.main.humidity").alias("humidity_percent"),
        col("parsed_data.wind.speed").alias("wind_speed_mps"),
        col("parsed_data.timestamp").alias("event_timestamp_ms"),
        current_timestamp().alias("processing_timestamp") # Thời điểm Databricks xử lý
    )

# ==========================================
# 4. GHI DỮ LIỆU VÀO DELTA LAKE
# ==========================================
output_path = "/delta/weather_data_stream" # Đường dẫn đến bảng Delta Lake trên DBFS/S3

# Ghi stream vào Delta Lake
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{output_path}/_checkpoints") \
    .option("path", output_path) \
    .trigger(processingTime="1 minute") \
    .start()

print(f"Databricks Structured Stream started for Kinesis stream: {kinesis_stream_name}")
print(f"Data is being written to Delta Lake at: {output_path}")