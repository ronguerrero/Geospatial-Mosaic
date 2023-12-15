# Databricks notebook source
# DBTITLE 1,Install Python Dependencies
# MAGIC %pip install geopandas contextily --quiet

# COMMAND ----------

from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

import contextily as cx
import fiona
import geopandas as gpd
import os
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Confirm shapefile is valid
fiona.listlayers(f"tl_rd22_13001_addrfeat.shp") # <- 'zip://' is required here

# COMMAND ----------

# DBTITLE 1,Read the shapefile using geopandas

#options: driver='shapefile', layer=0; also, 'zip://' is optional
gdf = gpd.read_file(f"tl_rd22_13001_addrfeat.shp")
print(f'rows? {gdf.shape[0]:,}, cols? {gdf.shape[1]}')
gdf.head()

# COMMAND ----------

# DBTITLE 1,Dig into crs data
gdf.crs

# COMMAND ----------

gdf_4326 = gdf.to_crs(epsg=4326)
gdf_4326.crs.to_string() # <- will be used with contextily

# COMMAND ----------

# DBTITLE 1,Plot the data
ax = gdf_4326.plot(column='ZIPL', cmap=None, legend=True, figsize=(20, 20), alpha=0.5, edgecolor="k")
cx.add_basemap(ax, zoom='auto', crs=gdf_4326.crs.to_string()) # <- specify crs!

# COMMAND ----------


