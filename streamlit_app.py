from collections import namedtuple
import altair as alt
import math
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px

# %%
county = pl.read_parquet('data/active_members_county.parquet')
tract = pl.read_parquet("data/active_members_tract.parquet")
chapel_scrape = pl.read_parquet("data/full_church_building_data-20.parquet")
chapel_safegraph = pl.read_parquet("data/safegraph_chapel.parquet")
temples = pl.from_arrow(pq.read_table("data/temple_details_spatial.parquet"))
tract_nearest = pl.from_arrow(pq.read_table("data/tract_distance_to_nearest_temple.parquet"))

"""
# Welcome to Streamlit!
Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:
If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).
In the meantime, below is an example of what you can do with just a few lines of code:
"""


with st.echo(code_location='below'):
    total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
    num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

    Point = namedtuple('Point', 'x y')
    data = []

    points_per_turn = total_points / num_turns

    for curr_point_num in range(total_points):
        curr_turn, i = divmod(curr_point_num, points_per_turn)
        angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
        radius = curr_point_num / total_points
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        data.append(Point(x, y))

    st.altair_chart(alt.Chart(pd.DataFrame(data), height=500, width=500)
        .mark_circle(color='#0068c9', opacity=0.5)
        .encode(x='x:Q', y='y:Q'))
