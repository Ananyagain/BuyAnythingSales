# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC create a widget for incremental run

# COMMAND ----------

dbutils.widgets.text("incremental flag", "0")

# COMMAND ----------

#check incremental flag
incremental_flag = dbutils.widgets.get("incremental flag")

# COMMAND ----------

#establish conn to adls

spark.conf.set("fs.azure.account.key.buyanythingsalesstorage.dfs.core.windows.net", "yp2MTrZ2gPkQXwKMoqwp2H+ACCbCi/fKNGzsS7UB3zv7M/PH1i8xko5ui+EbAlS5HXm+to/er+q7+ASt+2UVBA==")

# COMMAND ----------

# MAGIC %md
# MAGIC abfss:azure blob file system secure

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata`

# COMMAND ----------

# MAGIC %md
# MAGIC customer dim table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct SalesRegion from parquet.`abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata` 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC creating df-df_src

# COMMAND ----------

df_src = spark.sql("""
                   select distinct SalesRegion from parquet.`abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata`
                   """)
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC getting gold layer data -df_sink

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

path = 'abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_SalesRegion'
if DeltaTable.isDeltaTable(spark, path):
    df_sink=spark.sql("""
                   select dim_region_key, SalesRegion from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_SalesRegion`
                   """)
    print("Delta table exists")
else:
    # df_sink = spark.sql("""
    #                     select 1 as dim_cust_key, customerID, CustomerName, CustomerEmail, Country from parquet.`abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata` 
    #                     where 1=2
                        
    #                     """)
    df_sink=spark.createDataFrame([],schema="dim_region_key int, SalesRegion string")
#Create a DataFrame with zero rows but with the specified schema

df_sink.display()



# COMMAND ----------

# MAGIC %md
# MAGIC existing and new records
# MAGIC

# COMMAND ----------

df = df_src.join(df_sink, df_src.SalesRegion == df_sink.SalesRegion, "left").select(df_src.SalesRegion, df_sink.dim_region_key)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC existing records
# MAGIC

# COMMAND ----------

df_old = df.filter(df.dim_region_key.isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC new records

# COMMAND ----------

df_new = df.filter(df.dim_region_key.isNull())
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC get max dim_cust_key

# COMMAND ----------

print(incremental_flag)
print(type(incremental_flag))

# COMMAND ----------

if incremental_flag=="0":
    max_value = 1
else:
    max_value = df_old.select(max("dim_region_key")).collect()[0][0]

print(max_value)

# COMMAND ----------

# MAGIC %md
# MAGIC creating surrogate keys

# COMMAND ----------

df = df.withColumn("dim_region_key", max_value + monotonically_increasing_id())
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC union old and new

# COMMAND ----------

df_union = df_old.union(df)
df_union.display()


# COMMAND ----------

# MAGIC %md
# MAGIC upsert or SCD type 1
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

table_name = 'gold.dim_SalesRegion'
path = 'abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_SalesRegion'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark,path)
    deltaTable.alias("target").merge(df.alias("source"), 'target.dim_region_key = source.dim_region_key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print('table is updated')
else:
    df.write.format('delta').mode('overwrite').option('mergeSchema','true').save(path)
    print('table is created')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_SalesRegion`

# COMMAND ----------

