# databricks_notebook/kinesis_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from databricks.sdk import WorkspaceClient

# Khởi tạo Spark Session và dbutils
spark = SparkSession.builder.appName("KinesisConsumer").getOrCreate()
w = WorkspaceClient()
dbutils = w.dbutils

# =========================================
# 1. ĐỊNH NGHĨA SCHEMA (ĐÃ SỬA LỖI)
#    Khớp chính xác với JSON của OpenWeatherMap
# ==========================================
schema = StructType([
    StructField("coord", StructType([  # <-- SỬA LỖI: Đây là Struct
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([ # <-- SỬA LỖI: Đây là Array
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("wind", StructType([ # <-- SỬA LỖI: Đây là Struct
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", DoubleType(), True)
    ]), True),
    StructField("name", StringType(), True), # Tên thành phố
    StructField("cod", IntegerType(), True),
    StructField("timestamp", LongType(), True) # Timestamp chúng ta thêm vào
])

# ==========================================
# 2. ĐỌC DỮ LIỆU TỪ KINESIS STREAM (ĐÃ SỬA LỖI)
# ==========================================

# Lấy credentials một cách an toàn từ Databricks Secrets
aws_access_key = dbutils.secrets.get(scope="kinesis_creds", key="aws_access_key")
aws_secret_key = dbutils.secrets.get(scope="kinesis_creds", key="aws_secret_key")

kinesis_stream_name = "realtime-api-stream"
aws_region = "ap-southeast-1" 

df_kinesis = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("region", aws_region) \
    .option("initialPosition", "TRIM_HORIZON") \
    .option("awsAccessKey", aws_access_key) \
    .option("awsSecretKey", aws_secret_key)  \
    .load()

# ==========================================
# 3. XỬ LÝ DỮ LIỆU (ĐÃ SỬA LỖI)
# ==========================================
df_parsed = df_kinesis \
    .selectExpr("CAST(data AS STRING) as json_data") \
    .withColumn("parsed_data", from_json(col("json_data"), schema)) \
    .select(
        col("parsed_data.name").alias("city_name"),
        # SỬA LỖI: Lấy phần tử đầu tiên [0] của mảng "weather"
        col("parsed_data.weather")[0].main.alias("weather_condition"),
        col("parsed_data.weather")[0].description.alias("weather_description"),
        col("parsed_data.main.temp").alias("temperature_celsius"),
        col("parsed_data.main.humidity").alias("humidity_percent"),
        # SỬA LỖI: Truy cập "speed" bên trong "wind"
        col("parsed_data.wind.speed").alias("wind_speed_mps"),
        col("parsed_data.timestamp").alias("event_timestamp_ms"),
        current_timestamp().alias("processing_timestamp")
    )

# ==========================================
# 4. GHI DỮ LIỆU VÀO DELTA LAKE (Đã đúng)
# ==========================================
table_name = "workspace.default.weather_data_stream" 
checkpoint_path = "/Volumes/workspace/default/streaming_checkpoints/weather_data_stream_cp"

query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(table_name)

print(f"Databricks Structured Stream started for Kinesis stream: {kinesis_stream_name}")
print(f"Data is being written to Delta Table: {table_name}")