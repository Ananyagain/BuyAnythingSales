# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Establish connection and load data

# COMMAND ----------

#spark.conf.set("fs.azure.account.key.storageacc_name.dfs.core.windows.net", "access_key")
spark.conf.set("fs.azure.account.key.buyanythingsalesstorage.dfs.core.windows.net", "yp2MTrZ2gPkQXwKMoqwp2H+ACCbCi/fKNGzsS7UB3zv7M/PH1i8xko5ui+EbAlS5HXm+to/er+q7+ASt+2UVBA==")


# COMMAND ----------

#dbutils.fs.ls("abfss://container@storageacc_name.dfs.core.windows.net/folder")
filename=dbutils.fs.ls("abfss://bronze@buyanythingsalesstorage.dfs.core.windows.net/rawdata")[0].name 
#access just the filename
filename

# COMMAND ----------

df = spark.read.parquet(f"abfss://bronze@buyanythingsalesstorage.dfs.core.windows.net/rawdata/{filename}")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#initcap: capitalizes the first letter of each word
df = df.withColumn("Country", initcap(col("Country")))
df = df.withColumn("ProductCategory", initcap(col("ProductCategory")))
df = df.withColumn("ProductName", initcap(col("ProductName")))
df = df.withColumn("SalesRegion", initcap(col("SalesRegion")))
df = df.withColumn("CustomerName", initcap(col("CustomerName")))
df.display()


# COMMAND ----------

#remove rows where all values are missing
df=df.dropna(how='all')
df.display()

# COMMAND ----------

#remove duplicates
df=df.dropDuplicates()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC save file in silver folder
# MAGIC

# COMMAND ----------

df.write.format('parquet').mode('overwrite')\
    .option("path","abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata")\
    .save()

# COMMAND ----------

