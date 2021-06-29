
import csv
import logging
import json
import math
import random
import re
import time
import urllib.request
from pathlib import Path
import sys
import get_edgar.extractor.fileinfo_extractor as fileinfo


logger = logging.getLogger(__name__)

EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEC_PREFIX = "https://www.sec.gov"

def dl_file(csv_in,folder,info_folder=None):
    """Download webpage (sec filing)

    Arguments:
        csv_in {Path} -- the csv file containing the file information
        folder {Path} -- the folder to save the downloaded files

    Keyword Arguments:
        info_folder {Path} -- if folder to save the csv with info,
                                required if link does not exist in csv_in (default: {None})
    """ 
    if link_exist(csv_in):
        save_file(csv_in,folder)
    else:
        try:
            csv_info = fileinfo.save_file_info(csv_in,info_folder)
        except Exception:
            logger.exception(f'cannot add info for {csv_in.name}')
        else:
            save_file(csv_info,folder)

def link_exist(csv_in):
    """Examine whether there is link to the file in csv_in

    Arguments:
        csv_in {Path} -- the index or info csv

    Returns:
        Bool -- whether the link exists
    """
    with open(csv_in, 'r', newline='') as f:
        reader = csv.DictReader(f)
        if "htm_link" in reader.fieldnames:
            return True
        else:
            return False

def save_file(csv_in,folder):
    """Save all pages specifies in the csv_in

    Arguments:
        csv_in {Path} -- the csv files containing links for download
        folder {Path} -- the folder to save all downloaded files
    """
    with open(csv_in, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cik = row.get("cik")
                form_type = row.get("form_type")
                period = row.get("report_period")
            except Exception:
                logger.exception('infomration missing to save file')
            else:
                file_save = folder / f'f_{cik}_{form_type}_{period}.htm'    
                if file_save.exist() == False:
                    try:
                        page = read_page(row)
                        file_content = page.read()
                    except Exception:
                        logger.warning(f'cannot read file for {cik} {form_type} at {period}')
                    else:
                        with open(file_save, 'w') as f:
                            f.write(file_content)   
                else:
                    logger.info(f'file exists for {cik} {form_type} at {period}')   


def read_page(row):
    """Save webpage for each record

    Arguments:
        row {Dict} -- dictionary containing links and other information

    Returns:
        Page --  the webpage
    """
    try:
        return urllib.request.urlopen(row.get("htm_link"))            
    except Exception:        
        logger.exception(f'Wrong path {row.get("htm_link")} for \
        {row.get("cik")} {row.get("form_type")} at \
        {row.get("report_period")}')
        return None
