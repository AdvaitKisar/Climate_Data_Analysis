'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL YEARS (ZIP FILES) AND EXTRACTS THEM INTO RESPECTIVE FOLDERS CONTAINING CSV FILES
'''

# Importing libraries
import os
from zipfile import ZipFile

directory = 'Archive' # Name of the folder
for year in os.listdir(directory): # Iterating through each file (year) in the folder
    if year.endswith(".zip"):
        with ZipFile(os.path.join(directory, year), "r") as zip_ref:
            zip_ref.extractall(directory) # Extracts the zip file in the folder
            print(f"File {year} has been extracted successfully")