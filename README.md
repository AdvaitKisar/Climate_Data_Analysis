# IITM_BDL_Climate_Data_Analysis
This is a course project from the course CS5830: Big Data Laboratory offered at IIT Madras.

## Aim
This aim of this project is to create a pipeline to extract the Climate Data from [NCEI](https://www.ncei.noaa.gov/) website and find the monthly averages of different parameters across the globe and create plots to visualize this data.

## Procedure
The steps while executing this assignment are as follows:-
1. The first step is a pipeline to fetch the data from the website and store it in the archive. It is important to note that the data available is from 1901 to 2024 with more than 80 parameters and hourly readings with each year having details from anywhere between 5-6 stations to 10000+ stations. Hence, only the years 1901, 1911, ..., till 2001, and 2002 to 2024 were considered.
2. The next step is to arrange the data location-wise with the parameters in an array. This data was again stored in CSV files.
3. The third step is to compute the monthly averages for each month and parameters for each location. This data was also stored in CSV files.
4. The next step is to plot the data in the form of heatmaps.
5. Finally, an animated video is to be made which is optional.
