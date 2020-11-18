"""
title: "Merlin-py initial draft"
author: "Kellen O'Connor"
date: "January 2020"
"""

import os, shutil
import tabula
import pandas as pd
import numpy as np
from os.path import expanduser, getsize
home = os.path.expanduser('~')
pd.set_option('display.max_colwidth', 255)
import camelot
import queue
#import time


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

def generate_tblcntnts (path, field, avoid, results): #, tblcntntspath):
    index_results=page_index(path, field, avoid)
    table=create_tblcntnts_artifact(index_results)
    table['needed_pages'] = table['needed_pages'].astype(str)
    table['needed_pages'] = table['needed_pages'].str.replace(r"\[","")
    table['needed_pages'] = table['needed_pages'].str.replace(r"\]","")
    results.put(table)
    #table.to_csv(tblcntntspath)
    #return table

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
        #READD #print("Worker PDF Count: " + str(progress_count) + "/" + num_pdf + " on " + str(row['filename']))

        if ned_pg_len!=0:
            file=path+'/'+row['filename']
            tbls=camelot.read_pdf(file,pages=ned_pg)
            for  i in tbls:
                temp_df=i.df
                if temp_df.shape[1]==out_shape:
                    temp_df = temp_df.replace(to_replace='\n', value=' ', regex= True)
                    temp_df['file'] = row['filename']
                    temp_df.to_csv(output_path, mode='a', header='False')
                    #READD #print("Tables found matching selected shape in file "+row['filename'])
                else:
                    temp_df.to_csv(miss_path, mode='a', header='False')
                    #READD #print("No tables matching selected shape in file "+row['filename']+" despite being flagged in step 1, appending to missed output file...")
        else:
            #READD #print('Issue with '+row['filename']+'... Its likely that it didnt somehow meet the search critera.')
            missed_list.append(row['filename'])
        #cycle_end=time.clock()
        #print(cycle_end - cycle_time)
    print("OUT OF LOOP")



def input_data_split(table,threads=1):
    bins = [pd.DataFrame(columns = ['filename', 'filetype','non_990','needed_pages', 'needed_page_length'])]*threads

    if threads == 1:
        bins.append(table)

    #if threads == 2:
    #    table_length = len(table)
    #    middle_of_file = table_length / 2
    #    bin_1=table[:int(middle_of_file)]
    #    bin_2=table[int(middle_of_file):]

    else:
        table_length = len(table)
        table.sort_values(by='needed_page_length', ascending=False)
        bins_sizes = [0] * threads
        for index, row in table.iterrows():
            place = bins_sizes.index(min(bins_sizes))
            bins_sizes[place] += int(row['needed_page_length'])
            bins[place] = bins[place].append(row)

        #bin_size = int(table_length/threads)
        #current_file = 0
        #for i in range(threads):
        #    if i < threads - 1:
        #        bins.append(table[current_file : current_file + bin_size])
        #    else:
        #        bins.append(table[current_file:])
        #    current_file += bin_size

    return bins

def multi_run_wrapper(args):
    return data_pull(*args)


def extract(path, field, avoid, output, missedoutput, tableshape, threads, tblcntntspath):
    if threads == 1:
        results = queue.Queue()
        generate_tblcntnts(path=path, field=field,avoid=avoid, results=results) #, tblcntntspath=tblcntntspath)
        table = results.get()
        table.to_csv(tblcntntspath)
        data_pull(table=table,path=path,out_shape=tableshape,output_path=output,miss_path=missedoutput)
    #if threads == 2:
    else:
        table = pd.DataFrame(columns = ['filename', 'filetype','non_990','needed_pages', 'needed_page_length'])
        files = [f for f in os.listdir(path) if not f.startswith('.')]
        files.sort(key=lambda file: sort_files(file, path))
        for i in range(threads):
            os.mkdir(path+"/tmp"+str(i))
        dir_sizes = [0]*threads
        for file in files:
            dir = dir_sizes.index(min(dir_sizes))
            shutil.copy(path+'/'+file, path+"/tmp"+str(dir))
            dir_sizes[dir] += os.path.getsize(path+'/'+file)
        #table = generate_tblcntnts(path=path, field=field,avoid=avoid, tblcntntspath=tblcntntspath)
        from threading import Thread
        thread_list = [None]*threads
        results = queue.Queue()
        for i in range(threads):
            thread_list[i] = Thread(target=generate_tblcntnts, args=(path+"/tmp"+str(i), field, avoid, results)) #, tblcntntspath))
            thread_list[i].start()
        for i in range(threads):
            thread_list[i].join()
            shutil.rmtree(path+"/tmp"+str(i))
        while(results.qsize() > 0):
            result = results.get()
            table = table.append(result)
        table.to_csv(tblcntntspath)
        bins = input_data_split(table=table, threads=threads)
        data_pull_args = []
        for i in range(len(bins)):
            data_pull_args.append((bins[i], path, tableshape, output, missedoutput))
        from multiprocessing import Pool
        pool = Pool(threads)
        pool.map(multi_run_wrapper,data_pull_args)

def sort_files(file, path):
    return os.path.getsize(path+'/'+file)