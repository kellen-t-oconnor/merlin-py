

import os
import pandas as pd
home = os.path.expanduser('~')
pd.set_option('display.max_colwidth', 255)
from merlin_pull import *

### Setting merling_development folder as both a variable, and current working directory ###
merlin_dev=os.getcwd()

### This is here mostly as a safety just to preven multiprocessing from endlessly spawning additional threads ###
if __name__ == "__main__":

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

    extract(path=str(merlin_dev)+'/test_data',output=str(merlin_dev)+'/test_output/extracted_data.csv',
            missedoutput=str(merlin_dev)+'/test_output/missed_data.csv',field='dule I',avoid='example_avoid_value',
            tblcntntspath=str(merlin_dev)+'/test_output/tblcntnts.csv', tableshape=8, threads=5)

    #extract(path=str(merlin_dev)+'/demo_data',output=str(merlin_dev)+'/demo_output/extracted_data.csv',
    #        missedoutput=str(merlin_dev)+'/demo_output/missed_data.csv',field='dule I',avoid='example_avoid_value',
    #        tblcntntspath=str(merlin_dev)+'/demo_output/tblcntnts.csv', tableshape=8, threads=5)

###
#
#       General explanation of code flow:
#   All of the functions called by extract live in merlin_pull.py. The general order of execution goes something like this...
#   generate_tblcntnts() is first called to create the table of contents. To do so it calls create_tblcntnts_artifact, which then
#   immediately calls page_index() to do the initial page scan with tabula. Information then trickles upwards towards generate_tblcntnts()
#   first from page_index -> to create_tblcntnts_artifact() and then from create_tblcntnts_artifact() -> to generate_tblcntnts(). page_index()
#   starts with a few list objects and then eventually we get to a dataframe in generate_tblcntnts() that we eventually write to CSV, or
#   hold as a dataframe, or potentially both. This is where the multiprocessing logic begins...
#
#   if threads == 1 then the next step is very straigtforward. data_pull() begins by reading the table of contents document to pull data
#   from the flagged PDFs on the pages where the search term (field) showed up when scanned by tabula. Camelot will automatically pull ALL
#   tabular data from a page that is scanned. We filter out the tables we are not interested in by using the tableshape variable to weed out
#   tables that may exist on our wanted page, but don't have the same column dimensions. Eventually data_pull(), called by extracT() iterates
#   over every flagged document in the table of contents, and the code completes. When thread == 1 the code really iterates just like a series of
#   loops.
#
#   if threads == 2 then things are slightly more interesting. extract() calls input_data_split() to divide the table of contents in half based
#   on index position. For example if there were 100 documents flagged in a table of contents, bin 1 would be entries 1-50, and bin 2 would be
#   entries 51-100, etc. The code then calls on python's multiprocessing module to make 2 threads available for execution. the function multi_run_wrapper()
#   just allows the code to specify multiple arguments for each thread. The arguments for data_pull() only differ on the input data, though. These two
#   threads will then iterate over every item listed in the table of contents and will append to both output, and missedoutput until they are both
#   complete. because the order of how data is extracted in the documents (considering each observation has a recorded source filename as a variable), both
#   threads just continuously append to a shared output csv until completion.
#
###