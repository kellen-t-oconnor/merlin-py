import os
from os.path import expanduser
home = expanduser("~")
#os.chdir(home+'/PycharmProjects/tabular_pdf_scrape/modules')
import ocrmypdf
#import tesseract



def ocr(file_path, save_path):
    ocrmypdf.ocr(file_path, save_path, rotate_pages=True,
    remove_background=True,language="eng", deskew=True, force_ocr=True)

def ocr_directory(dir_path, output_path):
    files = os.listdir(dir_path)[0:]
    for i, file in enumerate(files):
        input_data=dir_path+file
        output_data=output_path+'processed_'+file
        print('Currently OCRing...'+file)
        ocr(input_data, output_data)