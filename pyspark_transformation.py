from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, avg
from pyspark.sql.window import Window
import os

# Create a Spark session
spark = SparkSession.builder.appName("GitHubTransformations").getOrCreate()

# Define blob details
account_name = os.getenv("USERNAME")
container_name = os.getenv("CONTAINER_NAME")
connection_string = os.getenv("CONN_STRING")
sas_token = os.getenv("SAS_TOKEN")
input_blob_name = "trending_repos_input.json"

input_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{input_blob_name}?sas_token={sas_token}"
df = spark.read.json(input_path)

# Transfromations -----------------------

# Filter repositories with more than 1000 stars
filtered_df = df.filter(df.stars > 1000)

# Calculate a new column for the ratio of forks to stars # df1 --> df_with_ratio
df_with_ratio = filtered_df.withColumn("forks_to_stars_ratio", col("forks") / col("stars"))

# Group by language and calculate average stars and forks # df2 --> agg_df
agg_df = df.groupBy("language").agg(avg("stars").alias("avg_stars"), avg("forks").alias("avg_forks"))

# Rank repositories within each language group based on stars # df3 --> ranked_within_group_df
windowSpec = Window.partitionBy("language").orderBy(desc("stars"))
ranked_within_group_df = df.withColumn("rank_within_group", row_number().over(windowSpec))

# Transformation --------------------------

# Write the transformed data back to Azure Blob Storage
output_blob_name = "trending_repos_output.parquet"
output_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/{output_blob_name}?sas_token={sas_token}"
ranked_within_group_df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
