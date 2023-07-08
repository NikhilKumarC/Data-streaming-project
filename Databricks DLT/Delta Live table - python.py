# Databricks notebook source
# MAGIC %pip install dlt

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %python
# MAGIC access_key = dbutils.secrets.get(scope="aws", key="access")
# MAGIC secret_key = dbutils.secrets.get(scope="aws", key="secreat")
# MAGIC encoded_secret_key = secret_key.replace("/", "%2F")
# MAGIC
# MAGIC spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",access_key)
# MAGIC spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",secret_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Clicks data to Bronze table

# COMMAND ----------

@dlt.create_table(
  comment="bronze table for clicks"
)
def clicks():
    schema='{"click_id": "493d829c-f2c6-4f8b-87b9-e6cd4cd7fc65", "user_id": 6, "product_id": 29, "product": "dinner", "channel": "gmail", "price": 57.85, "url": "https://boyle.com/search/", "user_agent": "Mozilla5.0 (compatible; MSIE 6.0; Windows NT 5.2; Trident 4.1)", "ip_address": "186.213.215.181", "datetime_occured": "2023-07-05 20:25:49.602"}'
    df=kinesis = spark.readStream.format("kinesis").option("streamName", "clicks").option("region", "us-east-1").option("initialPosition", "TRIM_HORIZON").option("awsAccessKey", access_key).option("awsSecretKey", secret_key).load().withColumn("click", F.from_json(F.col("data").cast("string"),schema_of_json(schema))).select("click.*")
    return df

# COMMAND ----------

@dlt.create_table(
  comment="bronze table for checkouts"
)
def checkouts():
    schema='{"checkout_id": "06d2ab9f-9363-414d-b06b-68761e0e16b6", "user_id": "88", "product_id": "13", "payment_method": "Discover", "total_amount": "83.46", "shipping_address": "9212 Rebecca Falls Perezland, CO 76764", "billing_address": "40304 Conway Lodge Suite 388 Port Catherineland, AS 95296", "user_agent": "Mozilla 5.0 (Macintosh; PPC Mac OS X 10_7_5) AppleWebKit/536.2 (KHTML, like Gecko) Chrome 14.0.892.0 Safari 536.2", "ip_address": "109.214.50.81", "datetime_occured": "2023-07-05 20:27:45.166"}'
    df=spark.readStream.format("kinesis").option("streamName", "checkouts").option(
        "region", "us-east-1"
    ).option("initialPosition", "TRIM_HORIZON").option("awsAccessKey", access_key).option(
        "awsSecretKey", secret_key
    ).load().withColumn("checkout", F.from_json(F.col("data").cast("string"),schema_of_json(schema))).select("checkout.*")
    return df

# COMMAND ----------


