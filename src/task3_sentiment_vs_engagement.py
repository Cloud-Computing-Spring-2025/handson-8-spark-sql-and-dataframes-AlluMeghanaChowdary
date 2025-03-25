from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col, round

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment scores
categorized_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Calculate average likes and retweets per sentiment category
sentiment_stats = (
    categorized_df
    .groupBy("Sentiment")
    .agg(
        round(avg("Likes"), 1).alias("Avg_Likes"),
        round(avg("Retweets"), 1).alias("Avg_Retweets")
    )
    .orderBy(col("Avg_Likes").desc())
)

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)