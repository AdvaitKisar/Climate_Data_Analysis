# IITM_BDL_Climate_Data_Analysis
This is a course project from the course CS5830: Big Data Laboratory offered at IIT Madras.

## Aim
This aim of this project is to create a pipeline to extract the Climate Data from [NCEI](https://www.ncei.noaa.gov/) website and find the monthly averages of different parameters across the globe and create plots to visualize this data.

## Procedure used in the Project
The steps while executing this assignment are as follows:-
1. The first step is a pipeline to fetch the data from the website using Airflow and store it in the archive. It is important to note that the data available is from 1901 to 2024 with more than 80 parameters and hourly readings with each year having details from anywhere between 5-6 stations to 10000+ stations. Hence, only the years 1901, 1911, ..., till 2001, and 2002 to 2024 were considered. This task is done by `DAG_task_1_archive_generator.py`.
2. The next two steps have to be executed using Airflow and Apache Beam for performing parallel operations and computations.
3. After downloading and zipping the files, the _unzipper_ class' function unzips the file year by year into respective folders.
4. After unzipping, the original data has to be refined using the _refining_archive_ class' function in `DAG_task_2_analytics_pipeline.py`. In simple words, it processes each year and location's file and extracts only the required data and fixes the mistyped characters, incompatible datatypes across rows, etc easing the further processing.
5. Once the locationwise data files were generated after refining, the monthly averages for all the ten parameters were calculated and saved locationwise into CSV files. This was performed using the _monthly_average_calculator_ class in `DAG_task_2_analytics_pipeline.py`.
6. The final data for all ten parameters was stored in CSV files with each of them consisting of station numbers, latitude, longitude, and the 12 columns containing their monthly averages for given parameter. This was executed using the _final_data_generator_ class in `DAG_task_2_analytics_pipeline.py`.
7. The final step is to plot the data in the form of heatmaps using geopandas. This has been implemented using the _plot_generator_ class in `DAG_task_2_analytics_pipeline.py`.
8. Finally, an animated video is to be made which is optional. This can be either done programmatically or using a software. This was done using Microsoft Clipchamp.

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
The plots or heatmaps for the ten parameters are included in the 'Plots' directory. For example, the heatmap of Hourly Sea Level Pressure for the month of January is shown below:-
![Hourly Sea Level Pressure Heatmap for Jan](https://github.com/AdvaitKisar/IITM_BDL_Climate_Data_Analysis/assets/85425955/1a924297-abf7-445f-bb4c-dcce1fa75fd6)
It can be observed that the sea level pressure in the coastal regions is lower as compared to the interiors of continents. Such observations can be studied using all plots.

## Animations
The animation for the Dry Bulb Temperature is plotted as shown below:-


https://github.com/AdvaitKisar/IITM_BDL_Climate_Data_Analysis/assets/85425955/0a459d6a-d872-495f-8e77-361c1e5ce637



In this animation, it can be observed that the yellow belt indicating higher temperatures oscillates between Northern and Southern Hemisphere which is the result of axial tilt of Earth leading to seasonal variations and temperature variations. Such animations help us to identify the global patterns which can help in understanding the global climate in much more detailed manner.

Do check out the plots in the 'Plots' directory and animations in the 'Animations' directory.
