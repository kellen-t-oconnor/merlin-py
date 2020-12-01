"""
title: "Merlin-py IRS downloader"
author: Matthew Barnes"
date: "November 2020"
"""


import os, shutil
import zipfile
from bs4 import BeautifulSoup
import requests
from merlin_pull import *

def IRSExtration(merlin_dev):
    # Set the url to download the 990 forms from
    url = 'https://www.irs.gov/charities-non-profits/form-990-series-downloads'
    print("Entered url: " + str(url))
    # urlExample is used to test, urlFull for full extraction
    urlExample = 'https://apps.irs.gov/pub/epostcard/990/2020/01/download990pdf_01_2020_prefixes_01-04'
    urlFull = 'https://apps.irs.gov/pub/epostcard/990/2020/01/download990pdf'

    # Get the full url html code
    r = requests.get(url)
    # Parse the html code to text
    soup = BeautifulSoup(r.text, 'html.parser')
    # Find all 'a' html tags
    all_hrefs = soup.find_all('a')
    # For all 'a' html tags, store an array of their 'hrefs'
    all_links = [link.get('href') for link in all_hrefs]
    # For all 'hrefs' in that array, make a new array containing only members that match the given string 
    zip_files = [dl for dl in all_links if dl and str(urlExample) in dl]
    #zip_files = [dl for dl in all_links if dl and str(urlFull) in dl]

    for zip_file in zip_files:
        extract_dir = os.path.splitext(os.path.basename(zip_file))[0]
        folder_dir = get_zip(zip_file, merlin_dev)
        ### This is the main function that will take you from point A to point B as far as extracting data goes. ###
        #
        #   PATH                = path to our input PDFs, the extract module (merlin_pull) only handles machine readable files
        #                         OCRing must be done with merlin_process_raw to OCR image files with tesseract.
        #   OUTPUT              = path to where your eventual output data will be written to.
        #   MISSEDOUTPUT        = path to where your collection of flagged yet somehow not scraped data will be dumped. This file
        #                         is very very messy usually.
        #   FIELD               = this is the term or field that you want to pull out, usually a label for a given table
        #   AVOID               = this is the term or field that you want tabula / camelot to avoid pulling
        #   TBLCNTNTSPATH       = path to where the code will write a csv file for your table of contents on that first initial
        #                         scan.
        #   TABLESHAPE          = the column dimensions of the tables you're interested in. Schedule I tables are 8 columns wide.
        #   THREADS             = the number of threads you want to use. right now the choices are just 1 or 2, logic can be expanded
        #                         to more if someone has access to a machine with many many cores though.
        #
        ###
        print("Zip cleanup finished. Starting processing: " + str(merlin_dev)+'\\test_data\\'+str(extract_dir))
        extract(path=str(merlin_dev)+'/test_data/'+str(extract_dir),output=str(merlin_dev)+'/test_output/extracted_data.csv',
                missedoutput=str(merlin_dev)+'/test_output/missed_data.csv',field='dule I',avoid='example_avoid_value',
                tblcntntspath=str(merlin_dev)+'/test_output/tblcntnts.csv', tableshape=8, threads=5)

        #extract(path=str(merlin_dev)+'/demo_data',output=str(merlin_dev)+'/demo_output/extracted_data.csv',
        #        missedoutput=str(merlin_dev)+'/demo_output/missed_data.csv',field='dule I',avoid='example_avoid_value',
        #        tblcntntspath=str(merlin_dev)+'/demo_output/tblcntnts.csv', tableshape=8, threads=5)
            
        # Delete extracted files before stating the next download
        print("Processing finished. Starting folder cleanup: " + str(folder_dir))
        shutil.rmtree(folder_dir)

# Used to download, extract and remove zip files
def get_zip (zip_file, merlin_dev):
    # Folder to save (and extract) the zip files to
    download_folder = merlin_dev + '/test_data'
    # If the directory doesn't exist, create ie
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    # Call the file the same as the download file
    zip_filename = os.path.basename(zip_file)
    # Tries are used to allow multiple attempts should one fail to reach the website
    tries = 0
    while tries < 3:
        # Attempt the download
        print("Starting download: " + str(zip_file))
        r = requests.get(zip_file)
        # Dictate the path to write to
        dl_path = os.path.join(download_folder, zip_filename)
        # Write out the file
        with open(dl_path, 'wb') as z_file:
            z_file.write(r.content)
        # Unzip the file
        print("Download finished. Starting extraction: " + str(zip_file))
        # extract_dir is the name of the folder 
        extract_dir = os.path.splitext(os.path.basename(zip_file))[0]
        # folder_dir is the name of the full path
        folder_dir = os.path.join(download_folder, extract_dir)
        try:
            # Extract all files in the zip file to the folder
            with zipfile.ZipFile(dl_path) as z:
                z.extractall(folder_dir)
            # Delete extracted zip file
            print("Extraction finished. Starting zip cleanup: " + str(zip_file))
            os.remove(dl_path)
            break
        except zipfile.BadZipfile:
            # the file didn't download correctly, so try again
            # this is also a good place to log the error
            pass

        tries += 1
    # Return the full path of the folder with the extracted files
    return folder_dir