# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#establish conn to adls

spark.conf.set("fs.azure.account.key.buyanythingsalesstorage.dfs.core.windows.net", "yp2MTrZ2gPkQXwKMoqwp2H+ACCbCi/fKNGzsS7UB3zv7M/PH1i8xko5ui+EbAlS5HXm+to/er+q7+ASt+2UVBA==")

# COMMAND ----------

# MAGIC %md
# MAGIC load all dimention tables

# COMMAND ----------

df_customer = spark.sql("""
                   select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_Customer`
                   """)
df_customer.display()

# COMMAND ----------

df_product = spark.sql("""
                   select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_Product`
                   """)
df_product.display()

# COMMAND ----------

df_date = spark.sql("""
                   select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_Date`
                   """)
df_date.display()

# COMMAND ----------

df_region = spark.sql("""
                   select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/dim_SalesRegion`
                   """)
df_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC load silver data

# COMMAND ----------

df_silver = spark.sql(
    """
    select * from parquet.`abfss://silver@buyanythingsalesstorage.dfs.core.windows.net/silverdata`
    """
)
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC creating the fact table

# COMMAND ----------

df_fact = df_silver.join(df_customer, df_silver.CustomerID == df_customer.customerID, "left")\
    .join(df_product, df_silver.ProductID == df_product.ProductID, "left")\
    .join(df_date, df_silver.OrderDate == df_date.OrderDate, "left")\
    .join(df_region, df_silver.SalesRegion == df_region.SalesRegion, "left")\
        .select(df_customer.dim_cust_key, df_product.dim_product_key, df_date.dim_date_key, df_region.dim_region_key, df_silver.UnitPrice, df_silver.TotalPrice, df_silver.Quantity)
df_fact.display()

# COMMAND ----------

from delta.tables import DeltaTable
# Existing code

path = 'abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/factSales'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark,path)
    deltaTable.alias("target").merge(df_fact.alias("source"), 'target.dim_cust_key = source.dim_cust_key and target.dim_product_key = source.dim_product_key and target.dim_date_key = source.dim_date_key and target.dim_region_key = source.dim_region_key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print('table is updated')
else:
    df_fact.write.format('delta').mode('overwrite').option('mergeSchema','true').save(path)
    print('table is created')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`abfss://gold@buyanythingsalesstorage.dfs.core.windows.net/factSales`

# COMMAND ----------

