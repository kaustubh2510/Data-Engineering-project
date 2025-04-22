import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, desc, to_date

#
# Make a Connection Between Data Bricks and Data Lake
#
spark.conf.set("fs.azure.account.auth.type.kdecommercedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.kdecommercedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.kdecommercedatalake.dfs.core.windows.net", "8027ce3c-badf-4a88-8874-48b7694db4e9")
spark.conf.set("fs.azure.account.oauth2.client.secret.kdecommercedatalake.dfs.core.windows.net","UCU8Q~Yx0YGYnaN3sk0t5oJCz7w~YeEh52rUZcDJ")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.kdecommercedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/9248a3fa-6400-4399-855a-17f6e02d4401/oauth2/token")



# --------------------------
# Load Data In DataBricks
# --------------------------
df = spark.read.format("csv").option("header", "true").load("abfss://bronze@kdecommercedatalake.dfs.core.windows.net/2019ecommdata")


# --------------------------
# 1 Total Views Per Brand
# --------------------------
brand_views = (
    df_spark.filter(col("event_type") == "view")
    .groupBy("brand")
    .agg(count("*").alias("total_views"))
    .orderBy(desc("total_views"))
)
# --------------------------
# Saving Data in DataBricks
# --------------------------
brand_views.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@kdecommercedatalake.dfs.core.windows.net/brand_views")\
            .save()


# --------------------------
# 2 Total Number of Purchases Per Brand
# --------------------------
brand_purchases = (
    df_spark.filter(col("event_type") == "purchase")
    .groupBy("brand")
    .agg(count("*").alias("total_purchases"))
    .orderBy(desc("total_purchases"))
)

# --------------------------
# Saving Data in DataBricks
# --------------------------
brand_purchases.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@kdecommercedatalake.dfs.core.windows.net/brand_purchases")\
            .save()

# --------------------------
# 3️ Total Purchase Amount Per Brand
# --------------------------
brand_purchase_amount = (
    df_spark.filter(col("event_type") == "purchase")
    .groupBy("brand")
    .agg(sum("price").alias("total_purchase_amount"))
    .orderBy(desc("total_purchase_amount"))
)

# --------------------------
# Saving Data in DataBricks
# --------------------------
brand_purchase_amount.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@kdecommercedatalake.dfs.core.windows.net/brand_purchase_amount")\
            .save()