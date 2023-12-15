# Databricks notebook source
# MAGIC %pip install netCDF4 xarray nc-time-axis cartopy --quiet

# COMMAND ----------

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np
import pandas as pd
import xarray as xr
     

# COMMAND ----------

# MAGIC
# MAGIC %sh 
# MAGIC # - again, this is single node
# MAGIC # - can just download to the driver and start working with it
# MAGIC wget https://www.unidata.ucar.edu/software/netcdf/examples/sresa1b_ncar_ccsm3-example.nc
# MAGIC ls -lh

# COMMAND ----------


ds = xr.open_dataset("sresa1b_ncar_ccsm3-example.nc")
print(ds)

# COMMAND ----------


ds.tas

# COMMAND ----------

# - Example-1: a simple plot
ax = plt.axes(projection=ccrs.PlateCarree()) # equidistance
ax.coastlines() 
ds.tas.plot()

# COMMAND ----------


