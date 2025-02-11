import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark
spark = SparkSession.builder.appName("GlueJsonTransform").getOrCreate()

# Input & Output Paths
INPUT_S3_PATH = "s3://de-youtubeanalysis-raw-useast1-dev/youtube/raw_statistics_reference_data/"
OUTPUT_S3_PATH = "s3://de-on-youtubeanalysis-cleaned-useast1-dev/scripts/"

# Read JSON data
df = spark.read.option("multiline", "true").json(INPUT_S3_PATH)

# Print schema (debugging)
df.printSchema()

# Ensure 'items' exists before exploding
if "items" in df.columns:
    df_flattened = df.select(explode(col("items")).alias("items")).select("items.*")
else:
    print("ERROR: 'items' column not found. Check JSON structure!")
    sys.exit(1)

# Ensure all expected columns are selected
df_final = df_flattened.select(
    col("video_id"),
    col("trending_date").cast("date"),
    col("title"),
    col("channel_title"),
    col("category_id").cast("int"),
    col("publish_time").cast("timestamp"),
    col("tags"),
    col("views").cast("bigint"),
    col("likes").cast("bigint"),
    col("dislikes").cast("bigint"),
    col("comment_count").cast("bigint"),
    col("thumbnail_link"),
    col("comments_disabled").cast("boolean"),
    col("ratings_disabled").cast("boolean"),
    col("video_error_or_removed").cast("boolean"),
    col("description")
)

# Write transformed data to S3 as Parquet
df_final.write.mode("overwrite").parquet(OUTPUT_S3_PATH)

print("✅ Data transformation completed and saved to S3.")
