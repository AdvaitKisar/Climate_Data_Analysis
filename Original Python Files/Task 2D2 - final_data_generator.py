'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL CSV FILES FROM THE MONTHLY AVERAGES AND CONSOLIDATES THE DATA PARAMETER-WISE, WHICH IS READY TO PLOT
INPUT DIR: Monthly Averages
OUTPUT DIR: Final Data
'''

# Importing libraries
import pandas as pd
import os
import numpy as np

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

    def store_field_details(self, input_directory, filename):
        '''
        Function:- Stores field details

        Inputs:- 
        self [object]: Instance of the current object
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

        
station_details = Station_Details() # Station Details are imported
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
    field_object = Field_Data_Generator(output_directory, field) # Field object is created
    for csv_iter, csv_file in enumerate(csv_files, start=1): # Iterating through stations
        print(f"Processing File No. {csv_iter}: {csv_file}")
        field_object.store_field_details(input_directory, csv_file) # Field data is stored
        print()
    field_object.clean_data() # Data is cleaned
    field_object.save_field_dataframe(output_directory) # Dataframe is saved for the field
    print("\n\n")
print(f"Data for all fields for all stations has been saved successfully in {output_directory}.")