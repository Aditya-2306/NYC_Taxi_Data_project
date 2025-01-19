# Databricks notebook source
# MAGIC %md
# MAGIC # Data Acesss

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.pandeynyctaxistorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.pandeynyctaxistorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.pandeynyctaxistorage.dfs.core.windows.net", "f73ef14f-bc41-4d3c-8543-313723d4719e")
spark.conf.set("fs.azure.account.oauth2.client.secret.pandeynyctaxistorage.dfs.core.windows.net", "47j8Q~_kfSTW_wb~go9bZrexbdW295vTnhGCVden")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.pandeynyctaxistorage.dfs.core.windows.net", "https://login.microsoftonline.com/92bb70dc-348a-4a63-bdad-98cb8098cee0/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Database Creation

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC # Data reading and writing in delta tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Storage variables

# COMMAND ----------

silver = 'abfss://silver@pandeynyctaxistorage.dfs.core.windows.net'
gold = 'abfss://gold@pandeynyctaxistorage.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC Data Zone

# COMMAND ----------

df_zone = spark.read.format("parquet")\
    .option("header", "true")\
        .option("inferschema", True)\
        .load('abfss://silver@pandeynyctaxistorage.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta') \
    .mode('append') \
    .option('path','abfss://gold@pandeynyctaxistorage.dfs.core.windows.net/trip_zone')\
    .save()\
    .saveAsTable('gold.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC use gold;
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC Trip type

# COMMAND ----------

df_type = spark.read.format("parquet")\
    .option("header", "true")\
    .option("inferschema", True)\
    .load(f'{silver}/trip_type') 

# COMMAND ----------

df_type.write.format('delta') \
    .mode('append') \
    .option('path','abfss://gold@pandeynyctaxistorage.dfs.core.windows.net/trip_type')\
    .save()\
    .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC Trips Data
# MAGIC

# COMMAND ----------

df_trip = spark.read.format("parquet")\
        .option("header", "true")\
        .option("inferschema", True)\
        .load(f'{silver}/trip_2023data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta') \
    .mode('append') \
    .option('path','abfss://gold@pandeynyctaxistorage.dfs.core.windows.net/trip_2023data')\
    .save()\
    .saveAsTable('gold.trip_2023data')

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_zone;

# COMMAND ----------

restore gold.trip_zone to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC