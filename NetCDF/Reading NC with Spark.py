# Databricks notebook source
# MAGIC %pip install "databricks-mosaic<0.4,>=0.3" --quiet

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management

# -- import databricks + spark functions

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# -- setup mosaic
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# -- other imports
import os

# COMMAND ----------


DBFS_DATA="/NetCDF_Coral/data"
ws_data = f"/dbfs/{DBFS_DATA}"
os.environ['WS_DATA'] = ws_data

# COMMAND ----------

# MAGIC %sh
# MAGIC # this is just in the workspace initially
# MAGIC mkdir -p $WS_DATA
# MAGIC

# COMMAND ----------

# MAGIC %sh 
# MAGIC # download all the nc files used
# MAGIC # - '-nc' means no clobber here
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220102.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220103.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220104.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220105.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220106.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220107.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220108.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220109.nc
# MAGIC wget -P $WS_DATA -nc https://github.com/databrickslabs/mosaic/raw/main/src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220110.nc
# MAGIC      

# COMMAND ----------

df = (
  spark
    .read.format("gdal")
      .option("driverName", "NetCDF")
    .load(DBFS_DATA)
)
print(f"count? {df.count():,}")
df.limit(1).show() # <- limiting display for ipynb output only

# COMMAND ----------

df_bleach = (
  df
    .repartition(df.count(), "tile")
    .select(
      mos
        .rst_getsubdataset("tile", F.lit("bleaching_alert_area"))
        .alias("tile")
    )
)
print(f"count? {df_bleach.count():,}")
df_bleach.limit(1).show() # <- `.display()` is prettier in databricks

# COMMAND ----------

df_subdivide = (
  df_bleach
    .repartition(df_bleach.count(), "tile") # <- repartition important!
    .select(
      mos
        .rst_subdivide(col("tile"), F.lit(8))
      .alias("tile")
    )
)
print(f"count? {df_subdivide.count():,}")   # <- go from 10 to 40 tiles
df_subdivide.limit(1).show()  

# COMMAND ----------


df_retile = (
  df_subdivide
    .repartition(df_subdivide.count(), "tile") # <- repartition important!
    .select(
      mos
        .rst_retile(col("tile"), F.lit(600), F.lit(600))
      .alias("tile")
    )
)
print(f"count? {df_retile.count():,}")         # <- go from 40 to 463 tiles
df_retile.limit(1).show()                      # <- `.display()` is prettier in databricks
     

# COMMAND ----------

(
    df_retile
    .repartition(df_retile.count(), "tile")
        .select(mos.rst_rastertogridavg("tile", F.lit(3)).alias("grid_avg"))
    .select(F.explode(col("grid_avg")).alias("grid_avg")) # <- explode-1 of 2d array
    .select(F.explode(col("grid_avg")).alias("grid_avg")) # <- explode-2 of 2d array
    .select(
        F.col("grid_avg").getItem("cellID").alias("h3"),      # <- h3 cellid
        F.col("grid_avg").getItem("measure").alias("measure") # <- coral bleaching
    )
    .createOrReplaceTempView("to_display")
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "to_display" "h3" "h3" 250_000
