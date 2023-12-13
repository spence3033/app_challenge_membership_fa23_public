# %%
from collections import namedtuple
import altair as alt
import math
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px

import folium
from streamlit_folium import folium_static

import matplotlib

import geopandas
import matplotlib.pyplot as plt
from geodatasets import get_path

import wget
import os

# %%
county = pl.read_parquet('data/active_members_county.parquet')
tract = pl.read_parquet("data/active_members_tract.parquet")
chapel_scrape = pl.read_parquet("data/full_church_building_data-20.parquet")
chapel_safegraph = pl.read_parquet("data/safegraph_chapel.parquet")
chapel_safegraph_clean = pl.read_parquet("data/safegraph_chapel_clean.parquet")
temples = pl.from_arrow(pq.read_table("data/temple_details_spatial.parquet"))
tract_nearest = pl.from_arrow(pq.read_table("data/tract_distance_to_nearest_temple.parquet"))



# Grabbing Lat and Longs for Tracts
tract_feature = pl.read_csv("./feature_scripts/features.csv", infer_schema_length=0)

tract_feature = tract_feature.with_columns(pl.col("total_home_owners").str.replace_all("null", 0))\
    .with_columns(pl.col("house_costs_total").str.replace_all("null", 0))\
    .with_columns(pl.col("average_housing_cost").str.replace_all("null", 0))


# tract_feature['total_home_owners'] = tract_feature['total_home_owners'].replace('null', None).astype('float64')
# tract_feature['house_costs_total'] = tract_feature['house_costs_total'].replace('null', None).astype('float64')

tract_feature = tract_feature.select(pl.col('tractcode').alias('home'), 
                    pl.col('total_home_owners').cast(pl.Int64),
                    pl.col('house_costs_total').cast(pl.Float64).cast(pl.Int64).alias('house_costs_total'),
                    pl.col('average_housing_cost').cast(pl.Float64))

# This is the tract dataframe with the Lat and Long values
tract_df = tract.join(tract_feature, on="home", how="left")

alt.data_transformers.disable_max_rows()
#%%
# Get the top 50 rows from active members from state tracts
chart = alt.Chart(tract_df.to_pandas())\
    .mark_bar().encode(
        x=alt.X("average_housing_cost", bin=alt.Bin(maxbins=30), title="average house costs in a tract"),
        y=alt.Y('count()', title=('number of tracts'))
    ).properties(
        width=650,
        height=400,
        title=f'Average housing costs for tracts'
    )
chart
#%%
tract_more_members = tract_df.filter(pl.col('active_members_estimate') > 50)\
                            .filter(pl.col('active_members_estimate') < 4000)
# Get the top 50 rows from active members from state tracts
chart = alt.Chart(tract_more_members.to_pandas())\
    .mark_circle(size=10).encode(
        x=alt.X("average_housing_cost", title='Average Housing Cost'),
        y=alt.Y('active_members_estimate', title=('Active Members'))
    ).properties(
        width=650,
        height=400,
        title=f'Active Membership compared to Tract\'s Average House Costs'
    )
chart

#%%
# # Grabbing Lat and Longs for Tracts
# tract_lat_longs = pl.read_csv("tract_lat_long.csv", infer_schema_length=0)
# tract_lat_longs_v2 = tract_lat_longs.rename({"GEOID": "home"})

# # This is the tract dataframe with the Lat and Long values
# tract_spatial = tract_df.join(tract_lat_longs_v2, on="home", how="left")

# # Spatial Map here 
# filtered = tract_spatial.filter(pl.col("home").str.starts_with(''))\

# tract_pd = filtered.to_pandas()

# betterUSA = geopandas.read_file(os.getcwd()+'/cb_2018_us_state_500k')


# # Plotting Adding this line
# fig, ax = plt.subplots()

# betterUSA.clip([-125, 30, -100, 50]).plot(ax=ax, color="white", edgecolor="black")

# gdf = geopandas.GeoDataFrame(
#     tract_pd, geometry=geopandas.points_from_xy(tract_pd['long'], tract_pd['lat']), crs="EPSG:4326"
# )

# # Clip and plot the GeoDataFrame on the same axis
# gdf.cx[-125:-100, 30:50].plot(ax=ax, column='average_housing_cost', markersize=0.5, legend=True, cmap='viridis', aspect=1)

# # Set plot titles
# ax.set_title('Average Housing Costs')

# # Display the Matplotlib figure using st.pyplot()
# fig

# %%
