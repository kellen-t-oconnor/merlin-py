"""
title: "Merlin-py initial draft"
author: "Kellen O'Connor"
date: "January 2020"
"""

import os
import tabula
import pandas as pd
import numpy as np
from os.path import expanduser
home = os.path.expanduser('~')
pd.set_option('display.max_colwidth', 255)
import camelot
import time


def page_index (path, field, avoid):
    folder = path
    files = os.listdir(folder)[0:]
    ID_L3 = []
    ID_L4 = []
    num_files = len([f for f in os.listdir(folder)if os.path.isfile(os.path.join(path, f))])
    progress_count=0
    for i, file in enumerate(files):
        path = folder + "/" + files[i]
        ID_L3.append("flagged")
        ID_L4.append(0)
    PAGEINDEX = []
    for i, file in enumerate(files):
        progress_count=progress_count+1
        path = folder + "/" + files[i]
        print("Generating table of contents... "+files[i]+"  "+str(progress_count)+"/"+str(num_files))
        ID_L5 = []
        ID_L6 = []
        y = []
        if ID_L4[i] == 1:
            PAGEINDEX.append(ID_L6)
        else:
            x = tabula.read_pdf(path, pages = "all", multiple_tables = True,
                                area=(0, 0, 30, 100), relative_area=True,
                                pandas_options={'header': None})
            if isinstance(x, list):
                for xpage in range(0,len(x)):
                    y.append(x[xpage].to_string())
                for ypage in range(0,len(y)):
                    if y[ypage].find(field) > -1 and y[ypage].find(avoid) == -1:
                        ID_L5.append(ypage)
                        ID_L6 = [p for p in ID_L5 if p > 23]
                temp = ID_L5 if len(ID_L5) > 0 else [1,2]
                temp.append(temp[-1] + 1)
                ID_L6 = ID_L5 if len(ID_L5)==0 else temp
                PAGEINDEX.append(ID_L6)
            else:
                PAGEINDEX.append(ID_L6)
    return files, ID_L3, ID_L4, PAGEINDEX


def create_tblcntnts_artifact (index_results):
    index_results=np.array(index_results, dtype=object)
    index_shape=index_results.shape
    field_d=index_shape[0]
    field_number=index_shape[1]
    table_pages=pd.DataFrame(columns = ['filename', 'filetype','non_990','needed_pages'])
    num = 0
    while num < field_number:
        filename=index_results[0][num]
        filetype=index_results[1][num]
        non_990=index_results[2][num]
        needed_pages=index_results[3][num]
        table_pages.loc[num] = [filename, filetype, non_990, needed_pages]
        num=num+1
    table_pages['needed_page_length']=table_pages['needed_pages'].str.len()
    return table_pages

def generate_tblcntnts (path, field, avoid, tblcntntspath):
    index_results=page_index(path, field, avoid)
    table=create_tblcntnts_artifact(index_results)
    table['needed_pages'] = table['needed_pages'].astype(str)
    table['needed_pages'] = table['needed_pages'].str.replace(r"\[","")
    table['needed_pages'] = table['needed_pages'].str.replace(r"\]","")
    table.to_csv(tblcntntspath)
    return table

def data_pull(table,path,out_shape, output_path, miss_path):
    missed_list=[]
    progress_count=0
    num_pdf=str(len(table))
    table.dropna(subset=['needed_pages'])
    for index, row in table.iterrows():
        ned_pg=row['needed_pages']
        ned_pg=str(ned_pg)
        if ned_pg=='nan':
            ned_pg=''
        ned_pg_len=len(ned_pg)

        progress_count = progress_count + 1
        print("Worker PDF Count: " + str(progress_count) + "/" + num_pdf + " on " + str(row['filename']))

        if ned_pg_len!=0:
            file=path+'\\'+row['filename']
            tbls=camelot.read_pdf(file,pages=ned_pg)
            for  i in tbls:
                temp_df=i.df
                if temp_df.shape[1]==out_shape:
                    temp_df = temp_df.replace(to_replace='\n', value=' ', regex= True)
                    temp_df['file'] = row['filename']
                    temp_df.to_csv(output_path, mode='a', header='False')
                    print("Tables found matching selected shape in file "+row['filename'])
                else:
                    temp_df.to_csv(miss_path, mode='a', header='False')
                    print("No tables matching selected shape in file "+row['filename']+" despite being flagged in step 1, appending to missed output file...")
        else:
            print('Issue with '+row['filename']+'... Its likely that it didnt somehow meet the search critera.')
            missed_list.append(row['filename'])
        cycle_end=time.clock()
        #print(cycle_end - cycle_time)



def input_data_split(table,threads=1):
    if threads == 1:
        bin_1=table

    if threads == 2:
        table_length = len(table)
        middle_of_file = table_length / 2
        bin_1=table[:int(middle_of_file)]
        bin_2=table[int(middle_of_file):]

    return bin_1, bin_2

def multi_run_wrapper(args):
    return data_pull(*args)


def extract(path, field, avoid, output, missedoutput, tableshape, threads, tblcntntspath):
    if threads == 1:
        table = generate_tblcntnts(path=path, field=field,avoid=avoid, tblcntntspath=tblcntntspath)
        data_pull(table=table,path=path,out_shape=tableshape,output_path=output,miss_path=missedoutput)
    if threads == 2:
        table = generate_tblcntnts(path=path, field=field,avoid=avoid, tblcntntspath=tblcntntspath)
        bin_1, bin_2 = input_data_split(table=table, threads=2)
        from multiprocessing import Pool
        pool = Pool(2)
        pool.map(multi_run_wrapper,[(bin_1, path, tableshape, output, missedoutput),
                                    (bin_2, path, tableshape, output, missedoutput)])
