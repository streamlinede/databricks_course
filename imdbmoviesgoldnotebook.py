# Databricks notebook source
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, year, month, dayofmonth, round


# COMMAND ----------

catalog = "dev"
silver_schema = "imdb_silver"
silver_tblName = "imdb_movies_consolidated"
gold_schema = "imdb_gold"
gold_tblName = "imdb_movies_aggr"

# COMMAND ----------

# from pyspark.sql.functions import current_date

# Get the current date
current_date_value = spark.sql("SELECT current_date()").collect()[0][0]
df_gold = spark.sql(f"select * from {catalog}.{silver_schema}.{silver_tblName} where run_date = '{current_date_value}' ")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_transformed = df_gold.withColumn("IMDB_Category",\
        when(col("IMDBScore").between(1,2.9), "Very Low") \
        .when(col("IMDBScore").between(3,4.9), "Low") \
        .when(col("IMDBScore").between(5,7.9), "Medium") \
        .otherwise("High"))

display(df_transformed)

# COMMAND ----------

df_aggr = df_transformed.withColumn('RuntimeInHours', round(df_transformed.Runtime/60,2)) \
                        .withColumnRenamed("Runtime","RuntimeInMins") \
                        .withColumn("Runtime_Category", when(col("RuntimeInHours").between(0,1.30),"Short Runtime")
                                    .when(col("RuntimeInHours").between(1.31,2.15) , "Medium Runtime")
                                    .otherwise("LongRuntime"))

# COMMAND ----------

display(df_aggr)

# COMMAND ----------

df_aggr.write.mode("append").format("delta").saveAsTable(f"{catalog}.{gold_schema}.{gold_tblName}")

# COMMAND ----------

df_refined = spark.table(f"{catalog}.{gold_schema}.{gold_tblName}")

# COMMAND ----------

df_refined.write.mode("overwrite").format("parquet").save("abfss://raw@streamlinebt1sa.dfs.core.windows.net/refined")

