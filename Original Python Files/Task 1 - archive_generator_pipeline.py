'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL YEARS AND FETCHES THEIR URL, SELECTS THE CSV FILES, FETCHES & DOWNLOADS THE CSV FILES AND CREATES A ZIP FILE
INPUT: NCEI Website (Web)
OUTPUT DIR: Archive 
'''

# Importing libraries
import os, requests, time, random, shutil
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_size(path):
    '''
    Function: To return the size of a file at a given path

    Inputs:-
    path: Location of the file

    Output:-
    Returns the size of the file in bytes if present else -1
    '''
    if os.path.isfile(path): # Checks if the file exists at the location
        return os.path.getsize(path)
    else:
        return -1
    
def basic_info():
    '''
    Function: Provides the basic info of data

    Input:- None

    Output:- 
    main_url[str]: returns the main URL of the website of NCEI i.e. parent directory
    '''
    main_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
    return main_url

def fetch_URL(main_url, year): # Task 1
    '''
    Function: Fetches the URL from the web for a particular year

    Inputs:-
    main_url [str]: Main URL of the NCEI website
    year [int]: Year for which the data needs to be retrieved

    Outputs:-
    response [requests object]: Contains the response of the website
    base_url [str]: URL of the data of particular year
    '''
    YYYY = str(year) + '/'
    base_url = urljoin(main_url, YYYY) # URL for the required year is made
    response = requests.get(base_url) # Response for the website is collected
    if response.status_code == 200: # Status Code 200 indicates that website can be accessed
        print(f"Website for {year} is accessible")
        return response, base_url
    else:
        print(f"Failed to access the website - Status Code: {response.status_code}")
        return -1
    
def select_files(response, year): # Task 2
    '''
    Function:- Selects files for a particular randomly

    Inputs:-
    response [requests object]: Contains the response of the website
    base_url [str]: URL of the data of particular year

    Outputs:-
    indices [list]: List containing indices of the selected files
    csv_links [list]: List of all csv links obtained by parsing the webpage
    '''
    # The HTML document of webpage of the particular year is parsed
    soup = BeautifulSoup(response.text, 'html.parser')
    # The CSV links are collected in a list from the parsed data
    csv_links = [a['href'] for a in soup.find_all('a', href=lambda href: (href and href.endswith('.csv')))]
    total_num_files = len(csv_links) # Total number of files on the webpage for a particular year
    # The number of files to be selected is set as the minimum of 100 and the number of total files
    # This is done to extract a subset of data which can be processed further.
    num_files = min(100, total_num_files)
    print(f"No. of files for the year {year} = {total_num_files}")
    indices = []
    while len(indices) < num_files:
        # An index is randomly picked from all the files available on the parsed webpage
        idx = random.randint(0, total_num_files-1) 
        # If the index is already picked earlier, the inner loop ensures that an unique index is picked each time such that none of the files are repeated.
        while idx in indices:
            idx = random.randint(0, total_num_files-1)
        indices.append(idx)
    return indices, csv_links

def fetch_files(indices, csv_links, base_url, year):
    '''
    Function:- To download the selected files and store them in the archive

    Inputs:-
    indices [list]: List containing indices of the selected files
    csv_links [list]: List of all csv links obtained by parsing the webpage
    base_url [str]: URL of the data of a particular year
    year [int]: Year for which the files need to be extracted

    Output:- None. Files are stored in the archive
    '''
    folder_size = 0 # Variable for calculating the size of folder of the given year
    print(f"Starting downloading files...\n")
    start = time.time()
    output_directory = str(year) # Directory for storing the CSV files
    os.makedirs(output_directory, exist_ok=True) # Creates the directory if not existing
    # Iterating through each of the selected files' indices
    for count, idx in enumerate(indices, start=1):
        csv_link = csv_links[idx] # CSV link for current index
        complete_url = urljoin(base_url, csv_link) # Constructing URL for this file
        filename = os.path.basename(complete_url) # Same filename is used
        csv_response = requests.get(complete_url) # Response of the CSV file on web is retrieved
        if csv_response.status_code == 200: # Proceeds if the file is available
            print(f"File no. {count}: {csv_link}  [Index: {idx}] is accessible")
            output_path = os.path.join(output_directory, filename) # Path for the CSV file to be stored
            with open(output_path, 'wb') as csv_file:
                csv_file.write(csv_response.content) # Writing the CSV data in the file
            print(f"Downloaded: {output_path}")
            file_size = (get_size(output_path))/(1024*1024) # Calculating file size in MB
            folder_size += file_size # Updating folder size
            print(f"Size of file: {file_size:.1f} MB")
            print(f"Size of folder {output_directory}: {folder_size:.1f} MB")
            print()
        else:
            print(f"Failed to download: {filename} - Status Code: {csv_response.status_code}")
            break
    end = time.time()
    total_num_files = len(csv_links)
    print(f"Downloaded {count} files out of original {total_num_files} files successfully.")
    print(f"Total time required: {((end-start)/60):.1f} minutes.")

def archive_folder(year):
    '''
    Function: Stores the folder in a zip file

    Input:-
    year [int]: Year for which the files have been extracted

    Output:- None. Zip file is stored.
    '''
    zip_filename = str(year)
    shutil.make_archive(zip_filename, 'zip', str(year)) # Zip archive is stored
    if os.path.exists(zip_filename + '.zip'): # Checks if file exists
        size = (os.path.getsize(zip_filename + '.zip'))/1024 # Calculates the size of Zip file in KB
        print(f"\nZIP file of size {size:.1f} KB for year {year} has been archived successfully.") 
    else:
        print("\nZIP file not created.")


# MAIN CODE
years = [1901+i for i in range(124)] # Years for which archive has to be made
main_url = basic_info() # Main URL is fetched
main_start = time.time()
for year in years: # Iterating for each year
    print(f"Downloading data for the year {year}")
    response, base_url = fetch_URL(main_url, year) # URL is fetched
    indices, csv_links = select_files(response, year) # Files are selected
    fetch_files(indices, csv_links, base_url, year) # Files are fetched and stored in a folder
    archive_folder(year) # Zip file is created
    print(f"Downloading data for year {year} completed.\n")
    curr_end = time.time()
    print(f"Time required till now: {((curr_end-main_start)/60):.0f} minutes.\n")
print(f"Total time for downloading entire archive: {((curr_end-main_start)/60):.0f} minutes.")