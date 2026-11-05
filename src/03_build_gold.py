# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 – Build gold
# MAGIC Business-ready outputs from silver.
# MAGIC
# MAGIC | Gold object | What it answers |
# MAGIC |---|---|
# MAGIC | `gold.rent_trend_zip` | Month-over-month and year-over-year rent change per ZIP |
# MAGIC | `gold.rent_value_zip` | Rent vs. home value and price-to-rent ratio per ZIP |
# MAGIC | `gold.v_austin_rent_drops` (view) | Austin ZIPs with the biggest year-over-year rent drops |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "housing_dev")
catalog = dbutils.widgets.get("catalog")

rent  = spark.table(f"{catalog}.silver.zori_tx")
value = spark.table(f"{catalog}.silver.zhvi_tx")

# COMMAND ----------

# Rent trend: window functions
w = Window.partitionBy("zip").orderBy("month")

rent_trend = (rent
    .withColumn("rent_last_month", F.lag("rent", 1).over(w))
    .withColumn("rent_last_year",  F.lag("rent", 12).over(w))
    .withColumn("mom_pct", F.round((F.col("rent") / F.col("rent_last_month") - 1) * 100, 2))
    .withColumn("yoy_pct", F.round((F.col("rent") / F.col("rent_last_year") - 1) * 100, 2)))

rent_trend.write.mode("overwrite").saveAsTable(f"{catalog}.gold.rent_trend_zip")

# COMMAND ----------

# Rent vs. home value: join + price-to-rent
rent_value = (rent
    .join(value, on=["zip", "month"], how="inner")
    .withColumn("price_to_rent",   F.round(F.col("home_value") / (F.col("rent") * 12), 1))
    .withColumn("gross_yield_pct", F.round(F.col("rent") * 12 / F.col("home_value") * 100, 2)))

rent_value.write.mode("overwrite").saveAsTable(f"{catalog}.gold.rent_value_zip")

# COMMAND ----------

# View: always reflects the latest data in rent_trend_zip
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.gold.v_austin_rent_drops AS
SELECT zip, City, month, rent, rent_last_year, yoy_pct
FROM {catalog}.gold.rent_trend_zip
WHERE Metro LIKE 'Austin%'
  AND month = (SELECT MAX(month) FROM {catalog}.gold.rent_trend_zip)
  AND yoy_pct IS NOT NULL
""")

for t in ["rent_trend_zip", "rent_value_zip"]:
    print(f"gold.{t}: {spark.table(f'{catalog}.gold.{t}').count():,} rows")