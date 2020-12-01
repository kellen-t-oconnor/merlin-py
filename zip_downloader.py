"""
title: "Merlin-py IRS downloader"
author: Matthew Barnes"
date: "November 2020"
"""


import os
import zipfile
import requests

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