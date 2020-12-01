import os
import requests
from bs4 import BeautifulSoup
import zip_downloader as zd
from merlin_pull import *

### Setting merling_development folder as both a variable, and current working directory ###
merlin_dev=os.getcwd()

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

for zip_file in zip_files:
    extract_dir = os.path.splitext(os.path.basename(zip_file))[0]
    folder_dir = zd.get_zip(zip_file, merlin_dev)

    print("Zip cleanup finished. Starting processing: " + str(merlin_dev)+'\\test_data\\'+str(extract_dir))
    extract(path=str(merlin_dev)+'/test_data/'+str(extract_dir),output=str(merlin_dev)+'/test_output/extracted_data.csv',
            missedoutput=str(merlin_dev)+'/test_output/missed_data.csv',field='dule I',avoid='example_avoid_value',
            tblcntntspath=str(merlin_dev)+'/test_output/tblcntnts.csv', tableshape=8, threads=5)

    # Delete extracted files before stating the next download
    print("Processing finished. Starting folder cleanup: " + str(folder_dir))
    shutil.rmtree(folder_dir)