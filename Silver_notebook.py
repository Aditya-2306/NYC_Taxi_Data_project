# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

Secret : "47j8Q~_kfSTW_wb~go9bZrexbdW295vTnhGCVden"
Application_id : "f73ef14f-bc41-4d3c-8543-313723d4719e"
Directory_id : "92bb70dc-348a-4a63-bdad-98cb8098cee0"


# COMMAND ----------

# MAGIC %md
# MAGIC from the above values we copied from our datastorage and ms entra id we just need to change the values in the given prewritten code to fetch the data

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.pandeynyctaxistorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.pandeynyctaxistorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.pandeynyctaxistorage.dfs.core.windows.net", "f73ef14f-bc41-4d3c-8543-313723d4719e")
spark.conf.set("fs.azure.account.oauth2.client.secret.pandeynyctaxistorage.dfs.core.windows.net", "47j8Q~_kfSTW_wb~go9bZrexbdW295vTnhGCVden")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.pandeynyctaxistorage.dfs.core.windows.net", "https://login.microsoftonline.com/92bb70dc-348a-4a63-bdad-98cb8098cee0/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC after we have changed the code with our needs we just need to write a command to fetch the data that is.
# MAGIC
# MAGIC dbutits --- initiator
# MAGIC
# MAGIC .fs.ls --- command to fetch the data
# MAGIC
# MAGIC then("the path of the file to be fetched")

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@pandeynyctaxistorage.dfs.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Reading CSV Data......****

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip type data**

# COMMAND ----------

df_trip_type = spark.read.format('csv').options(header='true', inferSchema='true').load('abfss://bronze@pandeynyctaxistorage.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Zone

# COMMAND ----------

df_trip_zone = spark.read.format('csv').options(header='true', inferSchema='true').load('abfss://bronze@pandeynyctaxistorage.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()
#it can also be done by 
#df_trip_zone.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data

# COMMAND ----------

# MAGIC %md
# MAGIC we can write the schema in two format that are 
# MAGIC
# MAGIC myschema = StructType([
# MAGIC   StructField(col1,IntegerType(),True)
# MAGIC ])
# MAGIC
# MAGIC ### and
# MAGIC
# MAGIC my_Schema = '''
# MAGIC
# MAGIC col1 int,
# MAGIC col2 bigint,
# MAGIC col3 float,
# MAGIC '''
# MAGIC
# MAGIC we use the second method because it is easy and fast to apply in the system.
# MAGIC
# MAGIC ### TIP : it is always advised to use your own schema when you are using RecursiveFileLookup 

# COMMAND ----------

myschema = '''VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RatecodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE
                '''

# COMMAND ----------

# when using different schema we change the inferschema to my_schema
df_trip = spark.read.format('parquet')\
    .options(header='true')\
    .option('recursiveFileLookup',True)\
    .schema(myschema)\
    .load('abfss://bronze@pandeynyctaxistorage.dfs.core.windows.net/trips_2023')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type**

# COMMAND ----------


df_trip_type = df_trip_type.withColumnRenamed('description','trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet').mode('append')\
.option('path','abfss://silver@pandeynyctaxistorage.dfs.core.windows.net/trip_type')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC **trip zone**

# COMMAND ----------

df_trip_zone.display()
# we are going to use a split fn to separate the zone column into 2 cols.

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('Zone1', split(col('Zone'), '/')[0])\
                           .withColumn('Zone2', split(col('Zone'), '/')[1]) 


df_trip_zone.display()  

# COMMAND ----------

df_trip_zone.write.format('parquet').mode('append')\
.option('path','abfss://silver@pandeynyctaxistorage.dfs.core.windows.net/trip_zone')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC # trip data

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date', to_date('lpep_pickup_datetime'))\
                .withColumn('trip_year', year('lpep_pickup_datetime'))\
                .withColumn('trip_month', month('lpep_pickup_datetime'))

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID', 'DOLocationID','fare_amount', 'total_amount')
df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
    .mode('append')\
    .option('path','abfss://silver@pandeynyctaxistorage.dfs.core.windows.net/trip_2023data')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

display(df_trip)