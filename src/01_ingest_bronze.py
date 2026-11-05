# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 – Ingest bronze
# MAGIC Incrementally loads new Zillow CSV files from the landing volume into bronze Delta tables with Auto Loader.
# MAGIC
# MAGIC | Source folder | Bronze table |
# MAGIC |---|---|
# MAGIC | `landing/raw/zori/` | `bronze.zori_raw` |
# MAGIC | `landing/raw/zhvi/` | `bronze.zhvi_raw` |

# COMMAND ----------

# Parameter: lets a job (or a dev/prod deployment later) choose the catalog
dbutils.widgets.text("catalog", "housing_dev")
catalog = dbutils.widgets.get("catalog")

raw = f"/Volumes/{catalog}/landing/raw"

# COMMAND ----------

def autoload_csv(source: str):
    """Load any new CSV files in raw/<source>/ into bronze.<source>_raw, then stop."""
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{raw}/_schemas/{source}")
        .option("cloudFiles.schemaHints", "RegionName STRING")   # keep ZIP leading zeros
        .load(f"{raw}/{source}/")
     .writeStream
        .option("checkpointLocation", f"{raw}/_checkpoints/{source}")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{source}_raw")
        .awaitTermination())

for source in ["zori", "zhvi"]:
    autoload_csv(source)
    print(f"{source}: {spark.table(f'{catalog}.bronze.{source}_raw').count():,} rows in bronze")