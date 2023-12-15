# Databricks notebook source
# MAGIC %pip install databricks-mosaic
# MAGIC

# COMMAND ----------

import os
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/geotiff/
# MAGIC cp MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF /dbfs/geotiff/

# COMMAND ----------

df = spark.read.format("gdal")\
    .option("driverName", "GTiff")\
    .load("/geotiff/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
df.show()
