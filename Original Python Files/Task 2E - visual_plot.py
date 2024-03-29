'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL FIELD FILES FROM THE FINAL DATA AND PLOTS THE HEATMAPS
INPUT DIR: Final Data
OUTPUT DIR: Plots
'''

# Importing libraries
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import colors
import os

def plot_heatmap(gdf, month, field, kwargs=None):
   '''
   Function:- Plot heatmap for given field and month

   Inputs:- 
   self [object]: Instance of the current object
   directory [str]: Directory where monthly averages are stored
   filename [str]: Filename

   Output:-
   field_data [list]: The data of the field is in a list
   '''
   fig, ax = plt.subplots(figsize=(20, 10))
   cmap = plt.cm.viridis

   # World map plotted in a lighter shade
   world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
   world = world.to_crs(epsg=4326)  # Project basemap to WGS84
   world.plot(ax=ax, linewidth=0.5, edgecolor='gray', color='lightblue', zorder=0)  # Plot world map first (zorder)
   # Data from geopandas df is plotted
   gdf.plot(column=month, ax=ax, linewidth=0.8, edgecolor='white', cmap=cmap, alpha=1, zorder=0,**kwargs)
   col_data = list(gdf[month].astype(float))
   vmin = min(col_data)
   vmax = max(col_data)

   # Colorbar specs and other plot setting
   sm = plt.cm.ScalarMappable(cmap=cmap, norm=colors.Normalize(vmin=vmin, vmax=vmax))
   sm.set_array([])
   plt.colorbar(sm, label=field, ax=ax, fraction=0.03)  # Add a label to the colorbar
   plt.grid('--', linewidth=0.5)
   plt.xlabel('Longitude [in deg]', fontsize=12)
   plt.ylabel('Latitude [in deg]', fontsize=12)
   plt.title(f'{field} Heatmap for {month}', fontsize=15)
   plt.show()
    
month_map = {
  1: 'Jan',
  2: 'Feb',
  3: 'Mar',
  4: 'Apr',
  5: 'May',
  6: 'June',
  7: 'July',
  8: 'Aug',
  9: 'Sep',
  10: 'Oct',
  11: 'Nov',
  12: 'Dec'
}

input_directory = 'Final Data'
output_directory = 'Plots'
os.makedirs(output_directory, exist_ok=True) # Output directory is created

# Dictionary of kwargs for plotting heatmaps
kwarg_dict = {
   'Hourly Altimeter Setting' : {'markersize': 25}, 
   'Hourly Dew Point Temperature': {'markersize': 20},
   'Hourly Dry Bulb Temperature': {'markersize': 20}, 
   'Hourly Relative Humidity': {'markersize': 20}, 
   'Hourly Sea Level Pressure': {'markersize': 20},
   'Hourly Station Pressure': {'markersize': 20},
   'Hourly Visibility': {'markersize': 20},
   'Hourly Wet Bulb Temperature': {'markersize': 20},
   'Hourly Wind Direction': {'markersize': 20},
   'Hourly Wind Speed': {'markersize': 20}
}

fields = list(kwarg_dict.keys())
for field in fields:
   folder = os.path.join(output_directory, field)
   os.makedirs(folder, exist_ok=True) # Folder for each field is created


field = fields[0] # Change the index for different fields
filename = field + '.csv'
path = os.path.join(input_directory, filename)
data = gpd.read_file(path) # Data imported as dataframe

geometry = gpd.points_from_xy(data['Longitude'], data['Latitude']) # Geometry column is generated and add the gdf
gdf = gpd.GeoDataFrame(data, geometry=geometry) # geopandas df
for month in month_map.values():
   plot_heatmap(gdf, month, field, kwarg_dict[field]) # Heatmaps are plotted
  