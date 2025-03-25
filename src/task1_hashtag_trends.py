from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split hashtags, explode into individual rows, count frequency
hashtag_counts = (
    posts_df
    .withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
    .withColumn("Hashtag", trim(col("Hashtag")))
    .groupBy("Hashtag")
    .count()
    .orderBy(col("count").desc())
    .limit(10)
)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
