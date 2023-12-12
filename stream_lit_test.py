#%%
import streamlit as st
import folium
import pandas as pd
import polars as pl
from streamlit_folium import folium_static

tract = pl.read_parquet("data/active_members_tract.parquet")

# tract_2 = tract.select(pl.col("home").cast(pl.UInt32))
# tract_geographic_data = pl.read_csv("data/cbg_geographic_data.csv", infer_schema_length=0)

#%%
tract_lat_longs = pl.read_csv("tract_lat_long.csv", infer_schema_length=0)
tract_lat_longs_v2 = tract_lat_longs.rename({"GEOID": "home"})

tract_df = tract.join(tract_lat_longs_v2, on="home", how="left")

state = "16"
filtered = tract_df.filter(
    pl.col("home").str.contains(f"^{state}")
)

# tract_df_v2 = tract_df.drop_nulls(["lat", "long"])
# tract_pd = tract_df_v2.to_pandas()
# tract_pd = tract_df.to_pandas()
tract_pd = filtered.to_pandas()




# # Convert the 'column_name' column from string to float
# tract_pd['lat'] = tract_pd['lat'].astype(float)
# tract_pd['long'] = tract_pd['long'].astype(float)

# map = folium.Map(location=[tract_pd['lat'].mean(), tract_pd['long'].mean()], zoom_start=8, control_scale=True)

# for index, row in tract_pd.iterrows():

#     folium.Marker([row["lat"], row["long"]], popup=row["home"]).add_to(map)

# folium_static(map)



#%%

import matplotlib

import geopandas
import matplotlib.pyplot as plt
from geodatasets import get_path

import wget
import os

#%%
# wget.download("https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip")
betterUSA = geopandas.read_file(os.getcwd()+'/cb_2018_us_state_500k')
# world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
# usa = world[world.name == 'United States of America']

# gdf = gdf.merge(df,left_on='STUSPS',right_on='state')

# %%
ax = betterUSA.clip([-155, 0, 0, 70]).plot(color="white", edgecolor="black")

# usa.plot(ax=ax)



# filtered_df = tract_pd.loc[tract_pd.home.str.contains(f'^{state}'), :]


gdf = geopandas.GeoDataFrame(
    tract_pd, geometry=geopandas.points_from_xy(tract_pd.long, tract_pd.lat), crs="EPSG:4326"
)

gdf.clip([-165, 0, 0, 74]).plot(ax=ax, column='active_members_estimate', markersize=0.5, legend=True, aspect='auto').set_title('active_members_estimate')

plt.show()
# %%
