'''
OBJECTIVE OF THIS FILE:-

THIS CODE ITERATES THROUGH ALL YEARS AND FETCHES THEIR URL, SELECTS THE CSV FILES, FETCHES & DOWNLOADS THE CSV FILES AND CREATES A ZIP FILE
INPUT: NCEI Website (Web)
OUTPUT DIR: Archive 
'''

# Importing airflow libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# Importing other libraries
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
    data_list [list]: returns the main URL of the website of NCEI i.e. parent directory and year
    '''
    main_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
    ## NOTE: Year needs to be changed manually
    year = 1901
    data_list = [main_url, year]
    return data_list

def fetch_URL_and_select_files(ti): # Task 1
    '''
    Function: Fetches the URL from the web for a particular year and selects the files

    Inputs:-
    ti: task instance

    Outputs:-
    data_list [list]: Contains main_url, year, base_url, indices and csv_links
    '''
    basic_info_op = ti.xcom_pull(task_ids=['task_basic_info'])
    data_list = basic_info_op[0]
    main_url = data_list[0]
    year = data_list[1]
    YYYY = str(year) + '/'
    base_url = urljoin(main_url, YYYY) # URL for the required year is made
    response = requests.get(base_url) # Response for the website is collected
    if response.status_code == 200: #  Status Code 200 indicates that website can be accessed
        print(f"Website for {year} is accessible")
    # The HTML document of webpage of the particular year is parsed
    soup = BeautifulSoup(response.text, 'html.parser')
    # The CSV links are collected in a list from the parsed data
    csv_links = [a['href'] for a in soup.find_all('a', href=lambda href: (href and href.endswith('.csv')))]
    total_num_files = len(csv_links)
    # The number of files to be selected is set as the minimum of 100 and the number of total files
    # This is done to extract a subset of data which can be processed further.
    num_files = min(100, total_num_files)
    print(f"No. of files for the year {year} = {total_num_files}")
    indices = []
    while len(indices) < num_files:
        # An index is randomly picked from all the files available on the parsed webpage
        idx = random.randint(0, total_num_files-1)
        while idx in indices:
            # If the index is already picked earlier, the inner loop ensures that an unique index is picked each time such that none of the files are repeated.
            idx = random.randint(0, total_num_files-1)
        indices.append(idx)
    data_list.extend([base_url, indices, csv_links])
    return data_list
    
def fetch_files(ti):
    '''
    Function: To download the selected files and store them in the archives

    Inputs:-
    ti: task instance

    Outputs:- None
    '''
    select_files_op = ti.xcom_pull(task_ids=['task_fetch_URL_and_select_files'])
    data_list = select_files_op[0]
    indices = data_list[3]
    csv_links = data_list[4]
    base_url = data_list[2]
    year = data_list[1]

    folder_size = 0 # Variable for calculating the size of folder of the given year
    print(f"Starting downloading files...\n")
    start = time.time()
    output_directory = str(year) # Directory for storing the CSV files
    os.makedirs(output_directory, exist_ok=True) # Creates the directory if not existing
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

def archive_folder(ti):
    '''
    Function: To download the selected files and store them in the archives

    Inputs:-
    ti: task instance

    Outputs:- None
    '''
    basic_info_op = ti.xcom_pull(task_ids=['task_basic_info'])
    data_list = basic_info_op[0]
    year = data_list[1]
    zip_filename = str(year)
    shutil.make_archive(zip_filename, 'zip', str(year)) # Zip archive is stored
    if os.path.exists(zip_filename + '.zip'): # Checks if file exists
        size = (os.path.getsize(zip_filename + '.zip'))/1024 # Calculates the size of Zip file in KB
        print(f"\nZIP file of size {size:.1f} KB for year {year} has been archived successfully.") 
    else:
        print("\nZIP file not created.")


# DAG Code
with DAG('task_1_archive_generator', start_date = datetime(2024, 1, 1),
         schedule_interval = "@daily", catchup = False) as dag:
        
        task_basic_info = PythonOperator(
              task_id = "task_basic_info",
              python_callable = basic_info
        )

        task_fetch_URL_and_select_files = PythonOperator(
              task_id = "task_fetch_URL_and_select_files",
              python_callable = fetch_URL_and_select_files
        )

        task_fetch_files = PythonOperator(
              task_id = "task_fetch_files",
              python_callable = fetch_files
        )

        task_archive_folder = PythonOperator(
              task_id = "task_archive_folder",
              python_callable = archive_folder
        )
        # Flow of code
        task_basic_info >> task_fetch_URL_and_select_files >> task_fetch_files >> task_archive_folder
