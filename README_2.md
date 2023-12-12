# Readme needs

4. Update your `readme.md` with details about your app and how to start it.
5. Include a link in your `readme.md` to your GitHub repository.

# Code Evaluation Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas, address the following items:_

You will review the `active_membership_disperse` Databricks files and document the following items. I am exploring your ability to digest and explain Pyspark code.

1. Explain what the code is doing in `Cmd`s 5, 22, 50, 60, 64, 66, and 74. Each `Cmd` will most likely require a couple of paragraphs. Please be detailed, but don't just write sentences describing each line.
2. Include an additional paragraph for each `Cmd` that proposes code improvements and any bugs that may be present.


<!-- ## My beginings at understanding the file -->
Beginning
1. Filter the safegraph places data for religious organizations and names that match our church name or other church names.
2. Drop Duplicate placekeys
3. Get pattern data, safegraph tract table, and safegraph spatial data which should only have the same placekeys as the places data.

Tables
poi, pattern, tract_table, spatial

Filtering Data (Cm 5)
4. Filter places data for Idaho data.
5. Again make sure spatial only has the same placekeys as places data.
6. Get spatial data for only Idaho.
7. In the patterns data, pull out month, year, and filter for only 2019.

8. Join patterns data with places data.
9. Drop year, month, visitor_country_of_origin. (I guess the year was only for filtering.)

10. Repeat 8, and 9 for idaho data.

11. filter tract_table for Idaho data.
12. Create SQL tables for poi, poi_idaho, spatial_idaho, spatial, pattern, pattern_idaho, tract_table

* Improvements or potential bugs - So far I don't see any possibilities for problems. I discovered safegraph pattern data can be replicated on an inner join but dropping duplicate placekeys should've fixed this problem. So at this point the data was just filtered and the data prepared to be put into SQL tables. The only improvement is the month didn't actually need to be touched and the spatial data didn't need to have a left_semi join on poi because that was already done in the code block above.  

SQL tables created -
    poi (For all churchs), 
    tract_table(For only Idaho), 
    spatial(For all churchs), 
    pattern (Joined poi and patterns data)
    poi_idaho, 
    spatial_idaho, 
    pattern_idaho ( Joined poi and patterns data)

Temple and Chapel Scrape Support
13. Get chapel data (might be scraped data on the church website)
14. Get temple data (Has to be scraped I'm pretty sure)
15. Read a file that has tract and distances to the nearest temple.
16. Add SQL tables for chapel_crape, temple_scrape, tract distance to neareast temple

SQL tables created -
    chapel_scrape
    temple_scrape
    tract_temple_distance

Explore tables - Skip
Traffic Filter to Chapel Algorithm

17. Prepare partition for placekeys and date_range_start and for them to be in descending order
...


All US Code (Cm 22)
1. Create partition with places over a month.
2. get pattern data
3. Explode the popularity_by_day column
4. Create a rank for popularity by days
5. Group all placekeys and days.
6. Get a percentile at the 70% for the rank values for a day. Then get the 70% percentile called rank_50th. Doesn't make sense but okay.
7. Filter out for rows with Sunday with the 70% quartile being in the top 3 day ranks or with Monday being in the bottom 4 ranks. 
8. Group placekeys. So The placekey should have Sunday and Monday information spread horizontally. It also may be missing Sunday or Monday information.
9. The value in the cells from the pivot will be the first rank_70th value found.
10. Filter out any placekeys where 70% of Mondays didn't end up in 4-7 rank. Or 70% of Sundays being in the top 1-3 rank.
11. Now join places data on the patterns data.
12. Lastly write this table into a SQL table.

US Spread Active Members to tracts(Cm 50)
1. Get chapel_nearest, get use_pattern_chapel
2. Join Fully on pattern and nearest (This means there could be null values for none fixes)
3. Filter for Sunday being the most used day, or if 2nd make sure dist is not null (I'm not sure why a distance has to be mentioned), or if distance is next to none and city does not exists (not sure the purpose for this. My guess is we want to use buildings nearly right on the spot where a scraped church building should be)
4. Drop duplicate placekeys

* We joined the nearest chapel to the patterns data for a point of interest. We filter for places that have sunday attendance as the highest, if second we check there is a dist given to the nearest chapel, lastly we keep an place that is almost right on a chapel location but check if city is null. I assume that means we save places that have no patterns data but are almost right on the same spot.

* 


Sunday Visitor Estimates (Cm 60)
1. Grab chapel plackeys, get patterns data for the chapels, get tract placekey map.
2. Join patterns data with chapels data.
3. Explode visits_by_day column
4. Join this exploded table with censustract_placekey map so we have the tract code connected to a placekey.
5. date column is made from date_range_start, it is also shifted some days later from the "pos" column. So what is happening is if the first day of a month has a pos (position) of 0, the 2nd day of a month has a pos of 1. So the data will become mm/02/yyyy. Day 3 mm/03/yyyy.
6. Pull out the day of the week.
7. filter all of the patterns data by a certain kind of day of the week. I assume that means "pos" was shifting all Sundays to equal "dayofweek" == 1
8. group the place data with all data for that month.
9. Count how many rows were added together, sum all other columns together, get the median for all other columns.
10. Sunday visits = Take the median value of visits on Sunday to a place during a month / total visits * normalized visits by state scaling. (So roughly the median number of visits on a month is the number of active members found in patterns data. If I feel 10 visits is the median for 100 visits over a month, roughly 1/10 of the normalized visits should be the members)
11. Now group all places data together to get the total members over the year and get a medain over the year, max during the year, min during the year, and months counted for the placekey.
12. Make table sunday_visits

home_dist
    - placekey
    - home
    - region
    - value
    - raw_vositor_counts
    - date_range_start
    - point_okacekey (e.g. lat & long)
    - dist (distance between lat & long and center_home * 69) - poi to home tract

Drop Distant Home Tract Visits (Cm 64)
1. Create partitions by placekeys
2. Create partitions by placekey and date range start so it groups month data.
3. Create partition by home (tract I think) and region.
4. Get table home_dist
5. For all values to a placekey during a month, create a sum of the visitors to different tracts that visited the poi.
6. Multiply the visitors at a tract to distance.
7. Sum the distances by visitors for a year.
8. sum all visitors to a placekey for a year.
9. Now get (distances * tract_visitors over a year / visitors over a year.) I guess this gets the average distance every visitor is away from a placekey.
10. Then get a standard distance deviation for each placekey.
11. Create a z-score which the distance differing from the mean distance away / standard deviation. (This now means if I live close to church and am the below the average distance away, I will have a negative z-score)
12. Filter out any tract visitors that are more than 2 standard deviations away from a church building.
13. Find the number of placekeys for every tract and region.
14. Find the number of date ranges for a tract and region.
15. Filter for places data that has only 1 month of data. And filter out tract values that only have 2 church buildings they go to or less.
16. Sum all visitors to a placekey for a month.
17. Drop any rows where 'value' = null I guess
18. Now group all placekeys and tract values.
19. Sum all visitors to a placekey over a year.
20. Sum the visitors from a tract for a single placekey for the year.
21. Count the value column being grouped together from a place.
22. Find the ratio of tract visitors / total visitors to a placekey.

Tract Active Member Estimates (Cm 66)
1. Get the home_weights table and join sunday visitor medians.
2. If the median of visits on a month is 15 or less, lets use the max visits to a church building. Else, lets use the median.
3. We get the active members by multiplying the median visits over a month by the home weight to a tract.
4. Lets group by tracts.
5. We'll round the sum of members to a tract.
6. Save this table as tract_membership

Final County Comparisons (Cm 74)
1. Finally, lets take this member estimate and join the expected census of lds members
2. Lets get the ratio of our expected estimates over the census of lds members.
3. Lets get the ratio of expected estimates over the total population.
4. Rename some columns.
5. Finally write this table as county_active_populations_ldscensus


All US Code (Cm 22)

US Spread Active Members to tracts(Cm 50, the first block right underneath)

Sunday Visitor Estimates (Cm 60)

Drop Distant Home Tract Visits (Cm 64 3rd code block underneath)

Tract Active Member Estimates (Cm 66)

Final County Comparisons (Cm 74, first block right underneath)

`Cmd`s 5, 22, 50, 60, 64, 66, and 74.



### Feature Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas include one spatial map of your feature and one feature vs target chart._ Note your charts could also be included in your app.

### Vocabulary/Lingo Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas, address the following items:_

1. Explain the added value of using DataBricks in your Data Science process (using text, diagrams, and/or tables).
2. Compare and contrast PySpark to either Pandas or the Tidyverse (using text, diagrams, and/or tables).
3. Explain Docker to somebody intelligent but not a tech person (using text, diagrams, and/or tables).

_Your answers should be clear, detailed, and no longer than is needed. Imagine you are responding to a client or as an interview candidate._

- _Clear:_ Clean sentences and nicely laid out format.
- _Detailed:_ You touch on all the critical points of the concept. Don't speak at too high a level.
- _Brevity:_ Don't ramble. Get to the point, and don't repeat yourself.





