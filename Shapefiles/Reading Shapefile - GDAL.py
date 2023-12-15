# Databricks notebook source
# MAGIC %md
# MAGIC Ensure mosaic-gdal-init.sh is specified as cluster init script

# COMMAND ----------

# MAGIC %pip install databricks-mosaic
# MAGIC

# COMMAND ----------

import os
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /dbfs/shapefiles
# MAGIC cp tl* /dbfs/shapefiles

# COMMAND ----------


df_kepler = (
  spark.read
    .format("shapefile")
    .option("asWKB", "false")
    .load(f"/shapefiles/tl_rd22_13001_addrfeat.shp")
  .withColumn("geom", mos.st_geomfromwkt("geom_0"))
  .withColumn("is_valid", mos.st_isvalid("geom"))
  .selectExpr(
    "fullname", "lfromhn", "ltohn", "zipl", "rfromhn", "rtohn", "zipr",
    "geom_0 as geom_wkt", "is_valid"
  )
)
print(f"count? {df_kepler.count():,}, num invalid? {df_kepler.filter('is_valid = False').count():,}")
df_kepler.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_kepler "geom_wkt" "geometry" 10_000 

# COMMAND ----------


