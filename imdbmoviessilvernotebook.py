# Databricks notebook source
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date, year, month, dayofmonth


# COMMAND ----------

catalog = "dev"
brz_schema = "imdb_bronze"
brz_tblName = "imdb_movies_raw"
silver_schema = "imdb_silver"
silver_tblName = "imdb_movies_consolidated"

# COMMAND ----------

from pyspark.sql.functions import current_date

# Get the current date
current_date_value = spark.sql("SELECT current_date()").collect()[0][0]
df = spark.sql(f"select * from {catalog}.{brz_schema}.{brz_tblName} where run_date = '{current_date_value}' ")

# COMMAND ----------

display(df)

# COMMAND ----------

df_transformed = df.withColumn("ParsedReleaseDate",
                               when(to_date(col("ReleaseDate"), "dd-MM-yyyy").isNotNull(),
                                    to_date(col("ReleaseDate"), "dd-MM-yyyy"))
                               .when(to_date(col("ReleaseDate"), "d/M/yyyy").isNotNull(),
                                     to_date(col("ReleaseDate"), "d/M/yyyy"))
                               .otherwise(None))  # This handles any unrecognized formats

# Show the results
display(df_transformed)

# COMMAND ----------

df = df_transformed.drop("ReleaseDate") \
    .withColumnRenamed("ParsedReleaseDate", "ReleaseDate")


# COMMAND ----------

# Handling missing values (example: replace null in 'IMDB Score' with a default value)
df_cleaned = df.fillna({"IMDBScore": 0.0})

# COMMAND ----------

# Check for duplicates based on 'Title' column
df_no_duplicates = df_cleaned.dropDuplicates(["Title"])

# Show the result after removing duplicates
display(df_no_duplicates)

# COMMAND ----------

# Convert string to DateType
df_transformed = df_no_duplicates.withColumn("AddedDate", to_date(col("AddedDate"), "dd-MM-yyyy"))

# Extract year and month from the 'ReleaseDate' column
df_transformed = df_transformed.withColumn("ReleaseYear", year(col("ReleaseDate"))) \
                               .withColumn("ReleaseMonth", month(col("ReleaseDate"))) \
                               .withColumn("ReleaseDay", dayofmonth(col("ReleaseDate")))

# Show the result
display(df_transformed)

# COMMAND ----------

df_transformed.write.mode("append").format("delta").saveAsTable(f"{catalog}.{silver_schema}.{silver_tblName}")
