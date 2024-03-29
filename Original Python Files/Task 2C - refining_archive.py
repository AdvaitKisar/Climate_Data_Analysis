'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL YEARS AND EXTRACTS THE ATMOSPHERIC PARAMETERS WHICH ARE TO BE PLOTTED AND CREATES A DATABASE OF STATION DETAILS
INPUT DIR: Archive
OUTPUT DIR: Refined Archive
'''

# Importing libraries
import pandas as pd
import os

class Station_Details():# Class for dealing with station details and related functions
    def __init__(self) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        filename = 'Station Details.csv'# Filename of the file containing Station Details
        columns = ['Station Number', 'Latitude', 'Longitude', 'Station Name'] # Columns in the file
        if not os.path.isfile(filename): # Checks for existing file
            df = pd.DataFrame(columns=columns)
            print("Creating new dataframe for Station Details")
        else: # If file exists, imports the station details
            df = pd.read_csv(filename)
            print("Data of Station Details fetched successfully.")
        
        self.df = df
        self.columns = columns
        self.filename = filename

    def store_station_details(self, station_no, lat, long, station_name):
        '''
        Function: Stores the station details in the dataframe

        Inputs:-
        self [object]: Instance of the current object
        station_no [int]: Station Number
        lat [float]: Latitude of the Station
        long [float]: Longitude of the Station
        station_name [str]: Name of the Station

        Output:-
        ind [int]: Indicator which is 1 when one station details are already in the database, else 0.
        '''
        if station_no in list(self.df.loc[:, 'Station Number']):
            print(f"The Station Details of Station No. {station_no} are already in the database")
            ind = 1
        else:
            new_row = pd.DataFrame([[station_no, lat, long, station_name]], columns=self.df.columns)
            self.df = pd.concat([self.df, new_row])
            print(f"Details of Station No. {station_no} added to the dataframe.")
            ind = 0
        return ind

    def save_station_dataframe(self):
        '''
        Function:- Saves the dataframe as csv file

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None. Saves the df as csv file
        '''
        self.df.to_csv(self.filename, header=True, index=False)
        print("Dataframe of Station Details has been stored successfully.")

class Extracting_Useful_Data():# Class for extracting the useful data which needs to be processed in downstream tasks
    def __init__(self, directory, filename) -> None:
        '''
        Function:- Initializes an object

        Inputs:- 
        self [object]: Instance of the current object
        directory [str]: Directory of the file where the data has to be stored
        filename [str]: CSV File's name for a particular station and year

        Output:- None
        '''
        fields = {
            'Hourly Altimeter Setting': 8,
            'Hourly Dew Point Temperature' : 9,
            'Hourly Dry Bulb Temperature': 10,
            'Hourly Relative Humidity': 15,
            'Hourly Sea Level Pressure': 17,
            'Hourly Station Pressure': 18,
            'Hourly Visibility': 19,
            'Hourly Wet Bulb Temperature': 20,
            'Hourly Wind Direction': 21,
            'Hourly Wind Speed': 23
        } # Dictionary of fields to be extracted from the downloaded data along with column numbers as values

        useful_columns = [i for i in range(4)]
        useful_columns.append(5)
        useful_columns.extend(fields.values()) # Indices of useful columns which are to be extracted

        path = os.path.join(directory, filename)
        data = pd.read_csv(path, usecols=useful_columns) # Data from CSV file is fetched
        print(f"The data from {path} has been imported.")
        self.data = data
        self.fields = fields
        self.filename = filename
        self.path = path
        pass

    def save_station_info(self, station_details):
        '''
        Function:- Saves the station details in the dataframe of Station Details

        Inputs:- 
        self [object]: Instance of the current object
        station_details [Station_Details]: Object containing station details

        Output:-
        ind [int]: Indicator which is 1 when one station details are already in the database, else 0.
        '''
        # self.data is the dataframe containing the data from CSV file of a particular station and year
        station_no = self.data.iloc[0, 0]
        station_name = self.data.iloc[0, 4]
        lat, long = self.data.iloc[0, 2], self.data.iloc[0, 3]
        print(f"Station No: {station_no}, Station Name: {station_name}")
        print(f"Lat = {lat:.2f}, Long = {long:.2f}")
        # The station details are saved using the store_station_details function of Station_Details object
        ind = station_details.store_station_details(station_no, lat, long, station_name)
        return ind

    def drop_columns(self):
        '''
        Function:- Drops the columns which are not needed

        Inputs:- 
        self [object]: Instance of the current object

        Output:- None
        '''
        self.data.drop(['STATION', 'LATITUDE', 'LONGITUDE', 'NAME'], axis=1, inplace=True)

    def replace_data_by_month(self):
        '''
        Function:- Replaces date entries by their month for better computation

        Input:- 
        self [object]: Instance of the current object
        
        Output:- None
        '''
        for i in range(len(self.data['DATE'])):
            s = self.data.loc[i, 'DATE'] # Extracting the string
            month = int(s[5:7]) # Extracting month
            self.data.loc[i, 'DATE'] = month # Setting it in the df
        self.data.rename(columns={'DATE':'MONTH'}, inplace=True) # Renaming the date column

    def save_df_to_csv(self, path):
        '''
        Function:- Saves the refined dataframe of a given station and year as CSV file

        Inputs:- 
        self [object]: Instance of the current object
        path [str]: Path of the folder

        Output:- None
        '''
        output_filename = os.path.join(path, self.filename) # Filename with path
        if not os.path.isfile(output_filename):
            # If file doesn't exist, it creates the CSV file
            self.data.to_csv(output_filename, header=True, index=False)
            print(f"Refined CSV File stored at {output_filename}.")
        else: # Else, it appends the data.
            # This ensures that the measurements at a given station for different years are collected in a single file having the name <STATION_NO>.csv
            self.data.to_csv(output_filename, mode='a', header=False, index=False)
            print(f"Refined Data appended to CSV File stored at {output_filename}.")


station_details = Station_Details() # Station Details are imported
output_directory = 'Refined Archive'
os.makedirs(output_directory, exist_ok=True) # Output is directory checked and created

years = [1901+10*i for i in range(11)] # Years from 1901, 1911, ..., 2001
years.extend([2002+i for i in range(23)]) # Years from 2002, 2003, ..., 2024.
directory = 'Archive' # The CSV Files (downloaded ones) are in this directory
stations_in_multiple_years = [] # List for detecting stations which have data of more than 1 year

for year in years: # Iterating through each year
    print(f'Refining data for the year: {year}\n')
    current_folder = str(year)
    path = os.path.join(directory, current_folder) # Path for the selected year is made
    csv_files = [f for f in os.listdir(path) if f.endswith(".csv")] # Names of CSV files are extracted for this year

    for iter, csv_file in enumerate(csv_files): # Iterating through each CSV file
        print(f"Iteration No. {iter+1}: Filename: {csv_file}")
        file_object = Extracting_Useful_Data(path, csv_file) # File object is created for extracting useful data
        ind = file_object.save_station_info(station_details) # Station details are saved in the Station Details database
        if ind == 1: # Detection of stations with mutliple occurences across years
            stations_in_multiple_years.append(csv_file)
        file_object.drop_columns() # Useless columns are dropped from imported data
        file_object.replace_data_by_month() # Dates are processed to months
        file_object.save_df_to_csv(output_directory) # The refined data is saved as a CSV file
        print()
    # Station details are saved for every iteration of outer for loop
    station_details.save_station_dataframe()
    print("\n\n")
print("Entire Archive has been refined and useful data is stored successfully.\n\n")
print("Printing stations with multiple occurences:-")
for file in stations_in_multiple_years:
    print(file)