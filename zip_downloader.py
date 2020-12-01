"""
title: "Merlin-py IRS downloader"
author: Matthew Barnes"
date: "November 2020"
"""


import os
import zipfile
import requests


def get_zip (zip_file, merlin_dev):
    download_folder = merlin_dev + '/test_data'

    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    zip_filename = os.path.basename(zip_file)
    tries = 0
    while tries < 3:
        print("Starting download: " + str(zip_file))
        r = requests.get(zip_file)
        dl_path = os.path.join(download_folder, zip_filename)
        with open(dl_path, 'wb') as z_file:
            z_file.write(r.content)

        # Unzip the file
        print("Download finished. Starting extraction: " + str(zip_file))
        # Use extract_dir to keep in seperate folders
        extract_dir = os.path.splitext(os.path.basename(zip_file))[0]
        folder_dir = os.path.join(download_folder, extract_dir)
        try:
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
    return folder_dir