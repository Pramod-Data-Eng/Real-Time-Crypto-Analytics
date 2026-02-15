from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read from the Glue Catalog (The table created by your Crawler)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "crypto_db", 
    table_name = "raw"
)

df = datasource.toDF()

# Extract Bitcoin data
btc_df = df.select(
    col("timestamp"),
    col("bitcoin.usd").alias("price_usd"),
    col("bitcoin.usd_24h_vol").alias("volume_24h")
).withColumn("symbol", lit("BTC"))

# Extract Ethereum data
eth_df = df.select(
    col("timestamp"),
    col("ethereum.usd").alias("price_usd"),
    col("ethereum.usd_24h_vol").alias("volume_24h")
).withColumn("symbol", lit("ETH"))

# Combine (Union) them into one clean dataset
final_df = btc_df.union(eth_df)

# Write to S3
final_df.write.mode("overwrite").parquet("s3://crypto-analytics-lake-2026/processed/crypto_prices/")

job.commit()