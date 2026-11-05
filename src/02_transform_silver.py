# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 – Transform silver
# MAGIC Reshapes bronze from wide (one column per month) to long (one row per ZIP per month), fixes types,
# MAGIC removes nulls and duplicates, and keeps Texas only.
# MAGIC
# MAGIC | Bronze | Silver |
# MAGIC |---|---|
# MAGIC | `bronze.zori_raw` | `silver.zori_tx` (zip, City, county, Metro, month, rent) |
# MAGIC | `bronze.zhvi_raw` | `silver.zhvi_tx` (zip, month, home_value) |

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "housing_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

def wide_to_long(bronze_table: str, value_name: str):
    """Unpivot month columns into rows and clean the result."""
    df = spark.table(bronze_table)
    month_cols = [c for c in df.columns if c.startswith("20")]

    return (df
        .unpivot(ids=["RegionName", "City", "Metro", "CountyName", "State"],
                 values=month_cols,
                 variableColumnName="month",
                 valueColumnName=value_name)
        .filter(F.col("State") == "TX")
        .withColumn("month", F.to_date("month"))
        .withColumn(value_name, F.round(F.col(value_name).cast("double"), 2))
        .filter(F.col(value_name).isNotNull())
        .withColumnRenamed("RegionName", "zip")
        .withColumnRenamed("CountyName", "county")
        .dropDuplicates(["zip", "month"]))

# COMMAND ----------

# Rent
zori = (wide_to_long(f"{catalog}.bronze.zori_raw", "rent")
        .select("zip", "City", "county", "Metro", "month", "rent"))
zori.write.mode("overwrite").saveAsTable(f"{catalog}.silver.zori_tx")

# Home values
zhvi = (wide_to_long(f"{catalog}.bronze.zhvi_raw", "home_value")
        .withColumn("home_value", F.round("home_value", 0))
        .select("zip", "month", "home_value"))
zhvi.write.mode("overwrite").saveAsTable(f"{catalog}.silver.zhvi_tx")

# COMMAND ----------

# Simple guardrail: fail the task (and stop the job) if silver came out empty
for t in ["zori_tx", "zhvi_tx"]:
    n = spark.table(f"{catalog}.silver.{t}").count()
    print(f"silver.{t}: {n:,} rows")
    dbutils.jobs.taskValues.set(key=f"{t}_rows", value=n)   # publish row count for later tasks
    assert n > 0, f"silver.{t} is empty – check bronze ingestion"