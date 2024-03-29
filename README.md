# IITM_BDL_Climate_Data_Analysis
This is a course project from the course CS5830: Big Data Laboratory offered at IIT Madras.

## Aim
This aim of this project is to create a pipeline to extract the Climate Data from [NCEI](https://www.ncei.noaa.gov/) website and find the monthly averages of different parameters across the globe and create plots to visualize this data.

## Procedure
The steps while executing this assignment are as follows:-
1. The first step is a pipeline to fetch the data from the website using Airflow and store it in the archive. It is important to note that the data available is from 1901 to 2024 with more than 80 parameters and hourly readings with each year having details from anywhere between 5-6 stations to 10000+ stations. Hence, only the years 1901, 1911, ..., till 2001, and 2002 to 2024 were considered.
2. The next two steps have to be executed using Airflow and Apache Beam for performing parallel operations and computations.
3. The next step is to arrange the data location-wise with the parameters in an array. This data was again stored in CSV files.
4. The third step is to compute the monthly averages for each month and parameters for each location. This data was also stored in CSV files.
5. The next step is to plot the data in the form of heatmaps. This is to be executed using geopandas and geodatasets.
6. Finally, an animated video is to be made which is optional. This can be either done programmatically or using a software.

## Parameters
The parameters plotted using the available data are as follows:-
1. Hourly Altimeter Setting
2. Hourly Dew Point Temperature
3. Hourly Dry Bulb Temperature
4. Hourly Relative Humidity
5. Hourly Sea Level Pressure
6. Hourly Station Pressure
7. Hourly Visibility
8. Hourly Wet Bulb Temperature
9. Hourly Wind Direction
10. Hourly Wind Speed

## Plots
The plots or heatmaps for the ten parameters are included in the 'Plots' directory. An example of the heatmap is shown below:-
![Alt text]([image link](https://github.com/AdvaitKisar/IITM_BDL_Climate_Data_Analysis/blob/main/Plots/Hourly%20Altimeter%20Setting/01%20Hourly%20Altimeter%20Setting%20Heatmap%20for%20Jan.png))
