# Databricks notebook source
catalog = "dev"
schema = "imdb_bronze"
tblName = "imdb_movies_raw"

# COMMAND ----------

from pyspark.sql.functions import current_date

# Get the current date
current_date_value = spark.sql("SELECT current_date()").collect()[0][0]
base_dir = f"abfss://raw@streamlinebt1sa.dfs.core.windows.net/ingest/{current_date_value}"
print(base_dir)
schema_columns = "Title string,Genre string,ReleaseDate string,Runtime int,IMDBScore double,Language string,Views int,AddedDate string"

# COMMAND ----------

df_raw = (spark.read.format("csv")
                    .schema(schema_columns)
                    .option("header", "true")
                    .load(base_dir))

# COMMAND ----------

display(df_raw)

# COMMAND ----------

df_raw = df_raw.withColumn("run_date", current_date())

# COMMAND ----------

df_raw.write.mode("append").saveAsTable(f"{catalog}.{schema}.{tblName}")
