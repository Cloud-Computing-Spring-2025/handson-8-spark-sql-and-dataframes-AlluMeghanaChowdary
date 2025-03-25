from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join datasets, filter verified users, calculate reach (Likes + Retweets)
top_verified = (
    posts_df.join(users_df, "UserID")
    .filter(col("Verified") == True)
    .groupBy("Username")
    .agg(_sum(col("Likes") + col("Retweets")).alias("Total_Reach"))
    .orderBy(col("Total_Reach").desc())
    .limit(5)
)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)