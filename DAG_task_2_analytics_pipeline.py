'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL CSV FILES IN THE ARCHIVE, CALCULATES MONTHLY AVERAGES FOR 10 PARAMETERS AND GENERATES PLOTS
'''

# Importing airflow libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib

# Importing other libraries
import os, re
from zipfile import ZipFile
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import colors

'''
THE CLASSES REPRESENT THE INDIVIDUAL TASKS AND THE SUBCLASSES AND FUNCTIONS ARE USED TO DISTRIBUTE THE SUBTASKS
'''

class Task_2_Unzipper:
    '''
    OBJECTIVE OF THIS CLASS:-

    THIS CODE ITERATES THROUGH ALL YEARS (ZIP FILES) AND EXTRACTS THEM INTO RESPECTIVE FOLDERS CONTAINING CSV FILES
    '''
    def main(ti):
        '''
        Main function of the Unzipper Class for task 1 which unzips the files in the archive
        '''
        file_sensor_result = ti.xcom_pull(task_ids='task_1_file_sensor') # Result of File sensor is imported
        if file_sensor_result != 'success':
            print('File could not be sensed. Pipeline stopped.')
            exit()
        directory = 'Archive' # Name of the folder
        for year in os.listdir(directory): # Iterating through each file (year) in the folder
            if year.endswith(".zip"):
                with ZipFile(os.path.join(directory, year), "r") as zip_ref:
                    zip_ref.extractall(directory) # Extracts the zip file in the folder
                    print(f"File {year} has been extracted successfully")

class Task_3_Refining_Archive:
    '''
    OBJECTIVE OF THIS CLASS:-

    THIS CODE ITERATES THROUGH ALL YEARS AND EXTRACTS THE ATMOSPHERIC PARAMETERS WHICH ARE TO BE PLOTTED AND CREATES A DATABASE OF STATION DETAILS
    INPUT DIR: Archive
    OUTPUT DIR: Refined Archive
    '''

    def __init__(self) -> None:
        pass

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

    def main(self):
        station_details = self.Station_Details() # Station Details are imported
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
                file_object = self.Extracting_Useful_Data(path, csv_file) # File object is created for extracting useful data
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

    def main_beam(self):
        beam_options = PipelineOptions(
            runner='DirectRunner',
            project='climate-analytics-pipeline',
            job_name='refining-archive',
            temp_location='/tmp',
            direct_num_workers=6,
            direct_running_mode='multi_processing'
        )
        with beam.Pipeline(options=beam_options) as pipeline:
            orig_func = self.main()
            result = (
                pipeline
                | beam.Map(orig_func)
            )

class Task_4A_Monthly_Average_Calculator:
    '''
    OBJECTIVE OF THIS CLASS:-

    THIS CODE ITERATES THROUGH ALL CSV FILES FROM THE REFINED ARCHIVE AND COMPUTES THE MONTHLY AVERAGES FOR ALL PARAMETERS AND STORES IT STATION-WISE.
    INPUT DIR: Refined Archive
    OUTPUT DIR: Monthly Averages
    '''

    def __init__(self) -> None:
        pass

    class Station_Details():# Class for dealing with station details and related functions
        def __init__(self) -> None:
            '''
            Function:- Initializes an object

            Inputs:- 
            self [object]: Instance of the current object

            Output:- None
            '''
            filename = 'Station Details.csv'
            df = pd.read_csv(filename) # Station details are imported
            print("Data of Station Details fetched successfully.")
            self.df = df
        
        def find_useless_files(self):
            '''
            Function:- Finds useless files in the database of Station details. Useless in this context mean the files which do not have any latitude or longitude mentioned

            Inputs:- 
            self [object]: Instance of the current object

            Output:- None
            '''
            n_stations = self.df.shape[0] # Number of unique stations
            useless_files = [] # List to store useless files
            for i in range(n_stations): # Iterating through stations
                if pd.isna(self.df.loc[i, 'Latitude']) == True or pd.isna(self.df.loc[i, 'Longitude']) == True: # Even if one of Latitude or Longitude is absent, such files are considered useless for further downstream tasks
                    useless_files.append(self.df.loc[i, 'Station Number'])
            print(f"Useless files = {len(useless_files)}")
            self.useless_files = useless_files

        def check_utility(self, filename):
            '''
            Function:- Checks the utility or usability of a file

            Inputs:- 
            self [object]: Instance of the current object
            filename [str]: Filename of station of the format <STATION_NO>.csv

            Output:-
            ind [int]: Indicator is 1 when file is useful else -1
            '''
            station_code = filename[:-4] # The station code (alphanumeric) is extracted from the filename
            if station_code.isdigit():
                # Numeric station codes are preprocessed as '01234' is saved in the useless_files list as '1234'.
                station_no = str(int(station_code))
            else:
                station_no = station_code
            if station_no in self.useless_files: # Checks if the file is in list of useless files
                ind = -1
                print(station_no)
            else:
                ind = 1
            return ind
    
    class Monthly_Average_Calculator(): # Class for calculating montly averages and storing them
        def __init__(self, directory, filename) -> None:
            '''
            Function:- Initializes an object

            Inputs:- 
            self [object]: Instance of the current object
            directory [str]: Directory where refined data is stored
            filename [str]: Filename of the form <STATION_NO>.csv

            Output:- None
            '''
            path = os.path.join(directory, filename) # Path is constructed
            data = pd.read_csv(path) # Refined Archive's Data for given filename is fetched as df
            print(f"The data from {path} has been imported.")
            self.data = data
            self.filename = filename
            pass

        def convert_and_extract(self, row, col):
            '''
            Function:- Extracts and converts a given cell value to float. This function was useful for extracting numerical values from mistyped strings like '32s', '-41.43a', etc.

            Inputs:- 
            self [object]: Instance of the current object
            row [int]: Row index
            col [int]: Column index

            Output:- 
            Extracted and converted float value
            '''
            var = self.data.iloc[row, col]
            try:
                return float(var)
            except ValueError:
                pass
            try:
                match = re.search(r"(\-?\d+\.?\d*)", var)
                if match:
                    return float(match.group(1))
            except ValueError:
                pass
            return None

        def calculate_monthly_average_per_param(self, col):
            '''
            Function:- Calculates monthly averages for a given parameter

            Inputs:- 
            self [object]: Instance of the current object
            col [int]: Column index of a given parameter e.g. Dew Point Temperature

            Output:-
            average_dict [dict]: Dictionary containing the average values of the parameter as the values of the dictionary and keys are the month numbers (1 - Jan, 2 - Feb, etc.)
            '''
            counter_dict = {i: 0 for i in range(1, 13)} # Dictionary for counting the number of entries for each month
            param_dict = {i: 0 for i in range(1, 13)} # Dictionary for summing up the entries for each month
            n_samples = self.data.shape[0] # Number of entries/samples
            if self.data.iloc[:, col].notna().any(): # If the entire column has atleast a non-null value, proceed...
                for i in range(n_samples): # Iterating through the entries
                    # If the entry is not a null value and if it is convertible to float, then proceed...
                    if pd.notna(self.data.iloc[i, col]) and self.convert_and_extract(i, col) != None:
                        month = int(self.data.iloc[i, 0]) # Extracting month
                        param_dict[month] += self.convert_and_extract(i, col) # Summing up the parameter's entry to respective month
                        counter_dict[month] += 1 # Updating count
                average_dict = {i: 0 for i in range(1, 13)} # Dictionary containing average values
                for key in average_dict.keys():
                    if counter_dict[key] == 0: # If count is 0, then the average is None
                        print(f"Count for {key}th month = 0")
                        avg = None
                    else:
                        avg = param_dict[key]/counter_dict[key]
                    average_dict[key] = avg
                return average_dict
            return None
        
        def calculate_MA_for_all_params(self, output_directory):
            '''
            Function:- Calculates monthly averages for all parameters

            Inputs:- 
            self [object]: Instance of the current object
            output_directory [str]: Output Directory

            Output:-
            None
            '''
            columns = self.data.columns # Columns for a station
            n_params = len(self.data.columns) - 1 # Number of parameters
            MA_array = np.zeros((12, n_params+1)) # 2D Array for storing monthly averages
            for i in range(12):
                MA_array[i, 0] = i+1 # Storing month numbers in 1st columns
            for col in range(1, n_params+1): # Iterating through each parameter
                op = self.calculate_monthly_average_per_param(col) # Output from function calculating the monthly averages for a single parameter
                if op:
                    average_dict = op
                    for month, avg in average_dict.items():
                        MA_array[month-1, col] = avg # Averages are saved in appropriate cells
            print(f"Calculated all monthly averages for the file {self.filename}")
            data_MA = pd.DataFrame(MA_array, columns=columns) # Pandas dataframe is created
            path = os.path.join(output_directory, self.filename)
            data_MA.to_csv(path, index=False) # Monthly averages for given station have been saved to a CSV file as <STATION_NO>.csv in the diretory 'Monthly Averages'
            print(f"Saved monthly averages at {path}.")

    def main(self):
        input_directory = 'Refined Archive'
        output_directory = 'Monthly Averages'
        os.makedirs(output_directory, exist_ok=True) # Output directory is created

        station_details = self.Station_Details() # Station details are retrieved
        station_details.find_useless_files() # Useless files are found
        print()
        csv_files = [f for f in os.listdir(input_directory) if f.endswith(".csv")] # CSV filenames are listed
        for iter, file in enumerate(csv_files, start=1): # Iterating through each filename
            print(f"Processing File No. {iter}: {file}")
            if station_details.check_utility(file) == -1: # Checking for usefulness of file
                print(f"File No. {iter}: {file} is useless.")
                continue
            file_object = self.Monthly_Average_Calculator(input_directory, file) # File object is created
            file_object.calculate_MA_for_all_params(output_directory) # Monthly averages are calculated and stored
            print()

    def main_beam(self):
        beam_options = PipelineOptions(
            runner='DirectRunner',
            project='climate-analytics-pipeline',
            job_name='monthly-average-calculation',
            temp_location='/tmp',
            direct_num_workers=6,
            direct_running_mode='multi_processing'
        )
        with beam.Pipeline(options=beam_options) as pipeline:
            orig_func = self.main()
            result = (
                pipeline
                | beam.Map(orig_func)
            )

class Task_4B_Final_Data_Generator:
    '''
    OBJECTIVE OF THIS CLASS:-

    THIS CODE ITERATES THROUGH ALL CSV FILES FROM THE MONTHLY AVERAGES AND CONSOLIDATES THE DATA PARAMETER-WISE, WHICH IS READY TO PLOT
    INPUT DIR: Monthly Averages
    OUTPUT DIR: Final Data
    '''

    def __init__(self) -> None:
        pass

    class Station_Details():# Class for dealing with station details and related functions
        def __init__(self) -> None:
            '''
            Function:- Initializes an object

            Inputs:- 
            self [object]: Instance of the current object

            Output:- None
            '''
            filename = 'Station Details.csv'
            df = pd.read_csv(filename) # Station details are imported
            print("Data of Station Details fetched successfully.")
            self.df = df

        def get_location(self, filename):
            '''
            Function:- Finds location (lat, long) of a station

            Inputs:- 
            self [object]: Instance of the current object
            filename [str]: Filename of station

            Output:-
            lat [float]: Latitude of Station
            long [float]: Longitude of Station
            '''
            station_code = filename[:-4] # The station code (alphanumeric) is extracted from the filename
            if station_code.isdigit():
                # Numeric station codes are preprocessed as '01234' is saved in the useless_files list as '1234'.
                station_no = str(int(station_code))
            else:
                station_no = station_code
            # Row number of station is found
            row_num = self.df[self.df['Station Number'].str.contains(station_no)]['Station Number'].idxmax()
            lat = self.df.loc[row_num, 'Latitude']
            long = self.df.loc[row_num, 'Longitude']
            return lat, long
    
    class Field_Data_Generator(): # Class for dealing with field/parameter data and related functions
        def __init__(self, directory, field) -> None:
            '''
            Function:- Initializes an object

            Inputs:- 
            self [object]: Instance of the current object
            directory [str]: Directory where monthly averages are stored
            field [str]: Field/parameter name

            Output:- None
            '''
            filename = field + '.csv'
            columns = ['Station Number', 'Latitude', 'Longitude']
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
            columns.extend([month_map[i+1] for i in range(12)]) # Column names
            path = os.path.join(directory, filename)
            if not os.path.isfile(path): # If file exists, will fetch existing data or else create new df
                df = pd.DataFrame(columns=columns)
                print(f"Creating new dataframe for {filename}")
            else:
                df = pd.read_csv(path)
                print(f"Data of {path} fetched successfully.")
            self.df = df
            self.columns = columns
            self.filename = filename
            self.field = field

        def fetch_field_details(self, directory, filename):
            '''
            Function:- Fetch field details

            Inputs:- 
            self [object]: Instance of the current object
            directory [str]: Directory where monthly averages are stored
            filename [str]: Filename

            Output:-
            field_data [list]: The data of the field is in a list
            '''
            path = os.path.join(directory, filename)
            data = pd.read_csv(path) # Data fetched and stored in df
            print(f"The monthly average data from {path} has been imported.")
            field = ''.join(self.field.split())
            field_data = data[field].tolist() # Field data is collected
            return field_data

        def store_field_details(self, station_details, input_directory, filename):
            '''
            Function:- Stores field details

            Inputs:- 
            self [object]: Instance of the current object
            station_details [Station_Details]: Datatype containing station details
            directory [str]: Directory where monthly averages are stored
            filename [str]: Filename

            Output:-
            field_data [list]: The data of the field is in a list
            '''
            lat, long = station_details.get_location(filename) # Lat, long are retrieved
            station_code = filename[:-4] # The station code (alphanumeric) is extracted from the filename
            if station_code.isdigit(): 
                # Numeric station codes are preprocessed as '01234' is saved in the useless_files list as '1234'.
                station_no = str(int(station_code))
            else:
                station_no = station_code

            if station_no in list(self.df.loc[:, 'Station Number']): # Checks if station details are already in the database
                print(f"The Field Details of Station No. {station_no} are already in the database")
                ind = 1
            else:
                field_data = self.fetch_field_details(input_directory, filename) # Field data is retrieved
                new_row = [station_no, lat, long]
                new_row.extend(field_data) # New row is created with current parameters monthly averages
                new_row = pd.DataFrame([new_row], columns=self.df.columns)
                self.df = pd.concat([self.df, new_row]) # It is appended to the existing df
                print(f"Field Details of Station No. {station_no} added to the dataframe.")
                ind = 0
            return ind
        
        def save_field_dataframe(self, output_directory):
            '''
            Function:- Saves field details as CSV file

            Inputs:- 
            self [object]: Instance of the current object
            output_directory [str]: Directory where final data has to be stored

            Output:- None
            '''
            path = os.path.join(output_directory, self.filename)
            self.df.to_csv(path, header=True, index=False) # Data saved in a CSV file at given path with field name
            print(f"Dataframe of {self.field} has been stored at {path} successfully.")

        def clean_data(self):
            '''
            Function:- Cleans data which removes null, empty and 0 values and fills them with average value of that particular month's numerical data

            Inputs:- 
            self [object]: Instance of the current object

            Output:- None
            '''
            print(f"Size before dropping empty rows: {self.df.shape}")
            useless_values = ['', None, 0] # Useless values
            cols = list(self.columns)[3:] # Cols to scan (Months)
            # AND statement
            self.df = self.df[
                (self.df['Jan'].isin(useless_values) == False) &
                (self.df['Feb'].isin(useless_values) == False) &
                (self.df['Mar'].isin(useless_values) == False) &
                (self.df['Apr'].isin(useless_values) == False) &
                (self.df['May'].isin(useless_values) == False) &
                (self.df['June'].isin(useless_values) == False) &
                (self.df['July'].isin(useless_values) == False) &
                (self.df['Aug'].isin(useless_values) == False) &
                (self.df['Sep'].isin(useless_values) == False) &
                (self.df['Oct'].isin(useless_values) == False) &
                (self.df['Nov'].isin(useless_values) == False) &
                (self.df['Dec'].isin(useless_values) == False)
                ]
            print(f"Size after dropping empty rows: {self.df.shape}")
            for col in cols:
                # All useless values are replaced by None
                self.df[col] = self.df[col].replace([0, ''], None)
                self.df[col] = self.df[col].replace('', None)
                numerical_values = self.df[col].dropna().astype(float) 
                avg = numerical_values.mean() # Average of numerical data
                self.df[col] = self.df[col].fillna(avg) # Null values replaced by average

    def main(self):
        station_details = self.Station_Details() # Station Details are imported
        fields = [
            'Hourly Altimeter Setting', 
            'Hourly Dew Point Temperature',
            'Hourly Dry Bulb Temperature', 
            'Hourly Relative Humidity', 
            'Hourly Sea Level Pressure',
            'Hourly Station Pressure',
            'Hourly Visibility',
            'Hourly Wet Bulb Temperature',
            'Hourly Wind Direction',
            'Hourly Wind Speed'
        ] # Fields for which the final data needs to be consolidated
        input_directory = 'Monthly Averages'
        output_directory = 'Final Data'
        os.makedirs(output_directory, exist_ok=True) # Created output directory
        csv_files = [f for f in os.listdir(input_directory) if f.endswith(".csv")] # CSV filenames are listed
        for field_iter, field in enumerate(fields, start=1): # Iterating through fields
            print(f"Field No. {field_iter}: {field}")
            field_object = self.Field_Data_Generator(output_directory, field) # Field object is created
            for csv_iter, csv_file in enumerate(csv_files, start=1): # Iterating through stations
                print(f"Processing File No. {csv_iter}: {csv_file}")
                field_object.store_field_details(station_details, input_directory, csv_file) # Field data is stored
                print()
            field_object.clean_data() # Data is cleaned
            field_object.save_field_dataframe(output_directory) # Dataframe is saved for the field
            print("\n\n")
        print(f"Data for all fields for all stations has been saved successfully in {output_directory}.")

    def main_beam(self):
        beam_options = PipelineOptions(
            runner='DirectRunner',
            project='climate-analytics-pipeline',
            job_name='final-data-generation',
            temp_location='/tmp',
            direct_num_workers=6,
            direct_running_mode='multi_processing'
        )
        with beam.Pipeline(options=beam_options) as pipeline:
            orig_func = self.main()
            result = (
                pipeline
                | beam.Map(orig_func)
            )

class Task_5_Plot_Generator:
    '''
    OBJECTIVE OF THIS CLASS:-

    THIS CODE ITERATES THROUGH ALL FIELD FILES FROM THE FINAL DATA AND PLOTS THE HEATMAPS
    INPUT DIR: Final Data
    OUTPUT DIR: Plots
    '''

    def __init__(self) -> None:
        pass

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

    def main(self):
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
            self.plot_heatmap(gdf, month, field, kwarg_dict[field]) # Heatmaps are plotted

    def main_beam(self):
        beam_options = PipelineOptions(
            runner='DirectRunner',
            project='climate-analytics-pipeline',
            job_name='plot-generation',
            temp_location='/tmp',
            direct_num_workers=6,
            direct_running_mode='multi_processing'
        )
        with beam.Pipeline(options=beam_options) as pipeline:
            orig_func = self.main()
            result = (
                pipeline
                | beam.Map(orig_func)
            )

# THE DAG RUNS THE FILE SENSOR AND MAIN FUNCTIONS OF EACH CLASS IN THE ORDER FIXED BELOW.
# THE PLOTS AND ANIMATION ARE SHARED IN THE MOODLE SUBMISSION AS ZIP FILES.
# THE ANIMATION WAS MADE USING MICROSOFT CLIPCHAMP (WINDOWS 11). NO CODE WAS USED FOR ANIMATION.
with DAG('task_2_analytics_pipeline', start_date = datetime(2024, 1, 1),
        schedule_interval = "@every_minute", catchup = False) as dag:

        task_1_file_sensor = FileSensor(
            task_id = 'task_1_file_sensor',
            fs_conn_id = 'Archive',
            filepath = '1901.zip',
            timeout = 5,
            poke_interval = 1
        )

        task_2_unzipper = PythonOperator(
              task_id = "task_2_unzipper",
              python_callable = Task_2_Unzipper().main
        )

        task_3_refine_archive = PythonOperator(
              task_id = "task_3_refine_archive",
              python_callable = Task_3_Refining_Archive().main
        )

        task_4A_calculate_monthly_avg = PythonOperator(
              task_id = "task_4A_calculate_monthly_avg",
              python_callable = Task_4A_Monthly_Average_Calculator().main
        )

        task_4B_generate_final_data = PythonOperator(
              task_id = "task_4B_generate_final_data",
              python_callable = Task_4B_Final_Data_Generator().main
        )

        task_5_generate_plots = PythonOperator(
              task_id = "task_5_generate_plots",
              python_callable = Task_5_Plot_Generator().main
        )

        task_1_file_sensor >> task_2_unzipper >> task_3_refine_archive >> task_4A_calculate_monthly_avg >> task_4B_generate_final_data >> task_5_generate_plots
