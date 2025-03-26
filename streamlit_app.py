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
county_before = pl.read_parquet('data/active_members_county.parquet')
tract_before = pl.read_parquet("data/active_members_tract.parquet")
chapel_scrape = pl.read_parquet("data/full_church_building_data-20.parquet")
chapel_safegraph = pl.read_parquet("data/safegraph_chapel.parquet")
chapel_safegraph_clean = pl.read_parquet("data/safegraph_chapel_clean.parquet")
# temples = pl.from_arrow(pq.read_table("data/temple_details_spatial.parquet"))
temples = pl.from_arrow(pq.read_parquet("data/temple_details_spatial.parquet"))
tract_nearest = pl.from_arrow(pq.read_table("data/tract_distance_to_nearest_temple.parquet"))

# %%
temples
#%%
# Address the following topics - explore answers by state, county, and/or tract.
# 1. How does the number of chapels in Safegraph compare to the number of chapels from the church website web scrape?
# 2. Does the active member estimate look reasonable as compared to the tract population?
# 3. Does the active member estimate look reasonable as compared to the religious census estimates by county?
# 4. How does the current temple placement look by state as compared to the county active membership estimates?

# App requirements
# 1. The ability to explore answers to these questions using an interactive filter with the state variable.
# 2. Spatial map with membership and temples shown on the map.
# 3. Tract distribution charts (boxplots and histograms) based on selected counties within a state.
# 4. The option to show the temples or hide the temples on the map.
# 5. A scaling input that allows the user to input a value between 0 and 1 that will adjust the active membership estimates proportionally.


# Submit by Dec 13, 2023
# A streamlit app
# A readme.md
# We submit a link to the app in Canvas
# We submit a .pdf/.html file in Canvas

# App Challenge - 
# Code Evaluation Challenge
# Feature Challenge - Complete with Pyspark on Databricks
# Vocabulary Challenge

st.header('Analyzing Active Members States')
st.write('The purpose of this app is to show active church members from safegraph\'s data from 2019.')

st.text('''

''')
st.subheader('Showing the data and visuals below')
#%%
statesList = {
    'Alabama':"01",         'Select All (Spatial map will load slowly)':'',         'Alaska':"02",          'Arizona':"04",	        
    'Arkansas':"05",	    'California':"06",	    'Colorado':"08",	    
    'Connecticut':"09",	    'Delaware':"10",	    'District of Columbia':"11",	
    'Florida':"12",	        'Georgia':"13",	        'Hawaii':"15",	        
    'Idaho':"16",	        'Illinois':"17",	    'Indiana':"18",
    'Iowa':"19",	        'Kansas':"20",	        'Kentucky':"21",	    
    'Louisiana':"22",	    'Maine':"23",	        'Maryland':"24",	    
    'Massachusetts':"25",	'Michigan':"26",	    'Minnesota':"27",	    
    'Mississippi':"28",	    'Missouri':"29",	    'Montana':"30",         
    'Nebraska':"31",        'Nevada':"32",          'New Hampshire':"33",
    'New Jersey':"34",      'New Mexico':"35",      'New York':"36",        
    'North Carolina':"37",  'North Dakota':"38",    'Ohio':"39",
    'Oklahoma':"40",        'Oregon':"41",          'Pennsylvania':"42",    
    'Puerto Rico':"72",     'Rhode Island':"44",    'South Carolina':"45",
    'South Dakota':"46",    'Tennessee':"47",       'Texas':"48",           
    'Utah':"49",            'Vermont':"50",         'Virginia':"51",
    'Virgin Islands':"78",  'Washington':"53",      'West Virginia':"54",   
    'Wisconsin':"55",       'Wyoming':"56"}

#%%
stateSelected = st.selectbox("Choose a State", statesList)
st.write("Selected: " + stateSelected)

active_member_porportion = st.slider("Active Membership Estimates Proportions", min_value=0.00, max_value=1.00, value=1.00, step=0.01)

county = county_before.with_columns(pl.col('active_members_estimate') * active_member_porportion)
tract = tract_before.with_columns(pl.col('active_members_estimate') * active_member_porportion)\
                    .with_columns((pl.col('active_members_estimate') / pl.col('population')).alias('proportion'))


# Grabbing Lat and Longs for Tracts
tract_lat_longs = pl.read_csv("tract_lat_long.csv", infer_schema_length=0)
tract_lat_longs_v2 = tract_lat_longs.rename({"GEOID": "home"})

# This is the tract dataframe with the Lat and Long values
tract_df = tract.join(tract_lat_longs_v2, on="home", how="left")

with st.expander('State Charts'):

    if st.button("Create State Charts"):
        st.subheader('Active Members')

        # Show all rows from the active members from the state tracts
        tract_filtered = tract.filter(pl.col("home").str.starts_with(statesList[stateSelected]))\
                            .sort("active_members_estimate", descending=True)
        tractpd_filtered = tract_filtered.to_pandas()

        # Get the top 50 rows from active members from state tracts
        chart_3 = alt.Chart(pd.DataFrame(tractpd_filtered).head(50))\
            .mark_bar().encode(
                x=alt.X("home", sort='-y'),
                y='active_members_estimate'
            ).properties(
                width=650,
                height=400,
                title=f'50 Top Tracts of Active Members - {stateSelected}'
            )
        chart_3

        chart_2 = alt.Chart(pd.DataFrame(tractpd_filtered)).mark_bar().encode(
            x=alt.X("home", sort='-y'),
            y='active_members_estimate'
        ).properties(
            width=650,
            height=400,
            title=f'All Tracts of Active Members - {stateSelected}'
        )
        chart_2


        st.subheader('Member ratios to Population')

        # Get the top 50 rows from active members from state tracts
        chart_4 = alt.Chart(pd.DataFrame(tractpd_filtered).head(50))\
            .mark_bar().encode(
                x=alt.X("home", sort='-y'),
                y='proportion'
            ).properties(
                width=650,
                height=400,
                title=f'50 Top Ratios of Members to Pop - {stateSelected}'
            )
        chart_4

        chart_5 = alt.Chart(pd.DataFrame(tractpd_filtered)).mark_bar().encode(
            x=alt.X("home", sort='-y'),
            y='proportion'
        ).properties(
            width=650,
            height=400,
            title=f'All Ratios of Members to Population - {stateSelected}'
        )
        chart_5

        
        tract_states = tract.with_columns(
            (pl.col('home').apply(lambda x: x[0:2]).alias('STATEFP'))
        )
        tract_states_names = tract_states.join(county, on="STATEFP", how="left")
        tract_states_filtered = tract_states_names.filter(pl.col('state_name').is_in(["Utah", "Idaho", "Wyoming", "Washington", stateSelected]))
        tract_statespd = tract_states_filtered.to_pandas()

        fig = px.box(tract_statespd, x='state_name', y="active_members_estimate", points="all")
        fig


with st.expander('County Charts'):

    if st.button("Create County Charts"):

        county_select = county.select(['STATEFP', 'COUNTYFP', 'county_name', 'state_name'])\
                            .filter(pl.col("STATEFP").str.starts_with(statesList[stateSelected]))

        countySelected = st.selectbox("Choose a County", county_select.select('county_name').to_pandas())
        st.write(f'Selected: {countySelected}')
        county_row = county_select.filter(pl.col('county_name') == countySelected)

        if not county_row.is_empty():
            countyfp = county_row['COUNTYFP'][0]
        
            # Get the rows for a County in a state.
            county_tracts = tract.filter(pl.col("home").str.starts_with(statesList[stateSelected] + countyfp))\
                                    .sort("active_members_estimate", descending=True)
            county_tractspd = county_tracts.to_pandas()


            # Get the top 50 rows from active members from state tracts
            chart_6 = alt.Chart(pd.DataFrame(county_tractspd))\
                .mark_bar().encode(
                    x=alt.X("home", sort='-y'),
                    y='active_members_estimate'
                ).properties(
                    width=650,
                    height=400,
                    title=f'Active members from {countySelected} - {stateSelected}'
                )
            chart_6

            chart_7 = alt.Chart(pd.DataFrame(county_tractspd)).mark_bar().encode(
                x=alt.X("home", sort='-y'),
                y='proportion'
            ).properties(
                width=650,
                height=400,
                title=f'Active Members ratios from {countySelected} - {stateSelected}'
            )
            chart_7

        else:
            st.write('No county code found')



    



lat_left = -165
lat_right = -50
long_bottom = 0
long_top = 75

with st.expander('Spatial Map'):
    st.subheader('Latitudes')
    lat_left = st.slider("Left", -165, -50, -165)
    lat_right = st.slider("Right", -165, -50, -50)

    st.subheader("Longitudes")
    long_top = st.slider("Top", 0, 75, 75)
    long_bottom = st.slider("Bottom", 0, 75, 0)
        
    # Spatial Map here 
    filtered = tract_df.filter(pl.col("home").str.starts_with(statesList[stateSelected]))\
                       .filter(pl.col("active_members_estimate") > 20)

    tract_pd = filtered.to_pandas()

    betterUSA = geopandas.read_file(os.getcwd()+'/cb_2018_us_state_500k')


    # Plotting Adding this line
    fig, ax = plt.subplots()

    betterUSA.clip([lat_left, long_bottom, lat_right, long_top]).plot(ax=ax, color="white", edgecolor="black")

    gdf = geopandas.GeoDataFrame(
        tract_pd, geometry=geopandas.points_from_xy(tract_pd['long'], tract_pd['lat']), crs="EPSG:4326"
    )

    # Clip and plot the GeoDataFrame on the same axis
    gdf.cx[lat_left:lat_right, long_bottom:long_top].plot(ax=ax, column='active_members_estimate', markersize=0.5, legend=True, cmap='viridis', aspect=1)


    on = st.toggle('See Temples')
    if on:
        dot_size = 2
        if lat_right - lat_left < 50:
            dot_size = 4
            if lat_right - lat_left < 25:
                dot_size = 6

        temples_pd = temples.filter(pl.col("STATEFP").str.starts_with(statesList[stateSelected])).to_pandas()
        gdf_2 = geopandas.GeoDataFrame(
            temples_pd, geometry=geopandas.points_from_xy(temples_pd['long'], temples_pd['lat']), crs="EPSG:4326"
        )
        # Clip and plot the GeoDataFrame on the same axis
        gdf_2.cx[lat_left:lat_right, long_bottom:long_top].plot(ax=ax, markersize=dot_size, aspect=1)

    st.write('I filter out tracts with active_member_estimates below 20')

    # Set plot titles
    ax.set_title('Active Member Estimates Map')

    # Display the Matplotlib figure using st.pyplot()
    st.pyplot(fig)

######### QUESTIONS ########
st.subheader('Important Questions')
with st.expander('How does the number of chapels in Safegraph compare to the number of chapels from the Church of Jesus Christ website?'):
    st.write('In safegraph we filtered down to 4,468 chapels.')
    st.write('There are 6,521 chapels in the United States found on the website for the Church of Jesus Christ of Latter-Day Saints.')

# Show all rows from the active members from the state tracts
tracts_utah = tract.filter(pl.col("home").str.starts_with('49'))\
                        .sort("active_members_estimate", descending=True)
tracts_utahpd = tracts_utah.to_pandas() 


# Show all rows from the active members from the state tracts
utah_counties = county.filter(pl.col("STATEFP") == '49')\
                        .sort("active_members_estimate", descending=True)\
                        .with_columns((pl.col('active_members_estimate') / pl.col('population')).alias('ratio_population_myCode'))\
                        .with_columns((pl.col('rcensus_lds') / pl.col('population')).alias('ratio_census_population'))
utah_countiespd = utah_counties.to_pandas()  

# Show all rows from the active members from the state tracts
tracts_NC = tract.filter(pl.col("home").str.starts_with('37'))\
                        .sort("active_members_estimate", descending=True)
tracts_NCpd = tracts_NC.to_pandas() 

# Show all rows from the active members from the North Carolina tracts
NC_counties = county.filter(pl.col("STATEFP") == '37')\
                        .sort("active_members_estimate", descending=True)\
                        .with_columns((pl.col('active_members_estimate') / pl.col('population')).alias('ratio_population_myCode'))\
                        .with_columns((pl.col('rcensus_lds') / pl.col('population')).alias('ratio_census_population'))
NC_countiespd = NC_counties.to_pandas() 

with st.expander('Does the active member estimate look reasonable as compared to the tract population?'):

    if st.button("View Answer To Question 2"):
    
        st.write('The proportions of active member estimates to population is the best indicator if the numbers feel off. In Utah the numbers should be very high and the graph below shows a few active member estimates going over the population; however, the proportion for active members in the counties balance out to being 5%-40% of the population. Since we had to estimate how chapel attendance dispersed accross tracts, it\'s expected some tracts will be a little lower or higher than they should be.')
    

        # Get proportions in Utah
        chart_8 = alt.Chart(pd.DataFrame(tracts_utahpd)).mark_bar().encode(
            x=alt.X("home", sort='-y'),
            y='proportion'
        ).properties(
            width=650,
            height=400,
            title=f'All Ratios of Members to Population - Utah'
        )
        chart_8


        # Get proportions in Utah
        chart_9 = alt.Chart(pd.DataFrame(utah_countiespd)).mark_bar().encode(
            x=alt.X("county_name", sort='-y'),
            y='ratio_population_myCode'
        ).properties(
            width=650,
            height=400,
            title=f'County Members to Population - Utah'
        )
        chart_9


        st.write('If we turn our focus to North Carolina. I am aware that there is one temple there. The graph shows many tracts have no members or only inactive members and that feels pretty good with me. That feels pretty good to me as I served a Spanish speaking mission there and church buildings for Spanish members served about 5-6 cities but I think there were members comming from every city. So seeing that only 5 counties had zero members comming to church, that seems correct.')   

        # Get proportions in Utah
        chart_10 = alt.Chart(pd.DataFrame(tracts_NCpd)).mark_bar().encode(
            x=alt.X("home", sort='-y'),
            y='proportion'
        ).properties(
            width=650,
            height=400,
            title=f'All Ratios of Members to Population - North Carolina'
        )
        chart_10
    

        # Get proportions in North Carolina
        chart_11 = alt.Chart(pd.DataFrame(NC_countiespd)).mark_bar().encode(
            x=alt.X("county_name", sort='-y'),
            y='ratio_population_myCode'
        ).properties(
            width=650,
            height=400,
            title=f'County Members to Population - North Carolina'
        )
        chart_11


with st.expander('Does the active member estimate look reasonable as compared to the religious census estimates by county?'):

    if st.button("View Answer To Question 3"):

        st.write('Now this ratio in Utah looks to low. Assuming our active member estimates are correct, this means only 10%-40% of members are active all across Utah. That looks like we have estimates that are too low.')

        # Get proportions in Utah
        chart_12 = alt.Chart(pd.DataFrame(utah_countiespd)).mark_bar().encode(
            x=alt.X("county_name", sort='-y'),
            y='ratio_census'
        ).properties(
            width=650,
            height=400,
            title=f'County Member ratio to lds members in the census  - Utah'
        )
        chart_12

        st.write('Here I want to compare the census of LDS members to population and weirdly one county had more lds members than the population which is obviously incorrect. As for the others, the numbers seem okay.')

        # Get proportions in Utah
        chart_13 = alt.Chart(pd.DataFrame(utah_countiespd)).mark_bar().encode(
            x=alt.X("county_name", sort='-y'),
            y='ratio_census_population'
        ).properties(
            width=650,
            height=400,
            title=f'County Member ratio to lds members in the census  - North Carolina'
        )
        chart_13

        st.write('To conclude, I think activity rate should be higher. What is likely messing up the numbers is safegraph is missing roughly 1500 chapels and a large chunk will be coming from Utah. So these are dropping the numbers to make Utah look bad based on their activity rates.')



with st.expander('How does the current temple placement look by state as compared to the county active membership estimates?'):

    if st.button("View Answer To Question 4"):

        temple_countydf = temples.select('temple', 'STATEFP', 'COUNTYFP').filter(pl.col('STATEFP').is_null() == False)\
                                .join(county.select('STATEFP', 'COUNTYFP', 'state_name', 'active_members_estimate'), on=['STATEFP', 'COUNTYFP'], how='left')
        
        countyGrouped = county.groupby('STATEFP', 'state_name').agg(pl.col('active_members_estimate').sum().alias('active_members_estimate'))

        temple_statedf = temples.select('temple', 'STATEFP', 'COUNTYFP').filter(pl.col('STATEFP').is_null() == False)\
                                .join(countyGrouped, on=['STATEFP'], how='left')

        temple_statedf_2 = temple_statedf.groupby('state_name', 'active_members_estimate').agg(pl.col('state_name').count().alias('temple_count'))\
                                        .with_columns((pl.col('active_members_estimate') / pl.col('temple_count')).alias('ratio_state_members_to_temples'))
        
        st.write('I\'m suprised how many temples have almost no active members in the county where it was built.')

        # Get proportions in Utah
        chart_14 = alt.Chart(pd.DataFrame(temple_countydf.to_pandas())).mark_bar().encode(
            x=alt.X("temple", sort='-y'),
            y='active_members_estimate'
        ).properties(
            width=650,
            height=400,
            title=f'County members where Temple is located'
        )
        chart_14

        st.write('If we compare the number of temples to active members. We can see how the numbers are starting to balance out. Utah isn\'t in the lead anymore, Arizona is. This feels right to me, some temples are placed in areas to shorten the time members have to travel to get to the temples so I\'d expect some low ratios, but overall each temple services thousands of members and even Utah and Idaho which have the highest number of members balance to almost the same as the other states.')

        # Get proportions in Utah
        chart_16 = alt.Chart(pd.DataFrame(temple_statedf_2.to_pandas())).mark_bar().encode(
            x=alt.X("state_name", sort='-y'),
            y='ratio_state_members_to_temples'
        ).properties(
            width=750,
            height=400,
            title=f'State members to Temple ratio'
        )
        chart_16