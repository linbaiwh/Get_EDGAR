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
import pandas as pd

import get_edgar.common.my_csv as mc

logger = logging.getLogger(__name__)

EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEC_PREFIX = "https://www.sec.gov"

## Download index

# Generate the output csv paths for the sample year
def dl_index(folder,start_year,end_year,form_types,prefix,ciks=None):
    """ Download index to csvs according 
        to the start year & end year

    Arguments:
        folder {Path} -- [the Path for the folder to store index csvs]
        start_year {int} -- [the start year of the sample period]
        end_year {int} -- [the end year of the sample period]
        form_types {string} -- [all the form types need to download index]
        prefix {string} -- [prefix of the output index csv names]
        ciks {Path or tuple or set} -- [csv file containing ciks needed, if applicable]

    Returns:
        [list of Paths] -- [list of Paths for all the index csvs during the sample period]
    """
    years = list(range(start_year, end_year+1))
    if folder.exists() == False:
        folder.mkdir()
    cik_need = input_cik(ciks=ciks)
    index_csvs = []
    for form_type in form_types:
        for year in years:
            index_csv = folder / f'index_{prefix}_{form_type}_{year}.csv'
            get_index_master(year,form_type,index_csv,cik_filter=cik_need)
            index_csvs.append(index_csv)
    return index_csvs

def input_cik(ciks=None):
    if ciks is not None: 
        if type(ciks) in (tuple,set):
            return ciks
        else:
            return mc.extract_obs(ciks,'CIK')
    else:
        return None

# Generate index csv for each year
def get_index_master(year, form_type, out_csv,cik_filter=None):
    """ Get index file for a form type during the specified years.
    year -> the year to download
    form_type -> the name of the form type required, case sensitive
    Output:
        csv file for required index
    """
    if out_csv.exists() == False:
        urls = index_url(year)
        with open(out_csv,'w', newline='') as out:
            writer = csv.writer(out)
            labels = ['cik', 'conm', 'form_type', 'filing_date','txt_path', 'html_index']
            writer.writerow(labels)
            for url in urls:
                try:              
                    master = urllib.request.urlopen(url).read()
                except urllib.error.HTTPError:
                    logger.error(f'{url} does not exist')
                    break
                lines = str(master, "latin-1").splitlines()
                for line in lines[11:]:# skip header, first 11 lines for master.idx
                    row = append_html_index(line)
                    if form_type_filter(row, form_type):
                        if cik_filter is not None:
                            if row[0] in cik_filter:
                                writer.writerow(row)
                        else:
                            writer.writerow(row)
                        
        logger.info(f"{year} {form_type} downloaded and wrote to csv")
        logger.info(f'{out_csv} created')
    else:
        logger.info(f'{out_csv} already exists')

def index_url(year):
    """ Generate url of the index file for future downloading.
    year - > the year to download
    Returns:
        url link of the index file
    """
    quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    return [f'https://www.sec.gov/Archives/edgar/full-index/{year}/{q}/master.idx' for q in quarters]

def append_html_index(line):
    """ Separate a line in an index file and Generate link of the index webpage.
    line - > a line in an index file
    Returns:
        a list of chunks in a line of an index file, including the index webpage
    """
    chunks = line.split("|")
    chunks[-1] = EDGAR_PREFIX + chunks[-1]
    chunks.append(chunks[-1].replace(".txt", "-index.html"))
    return chunks


def form_type_filter(chunks, form_type):
    """ Find a specific form type in the index file.
    chunks - > a seprated line in an index file
    form_type - > the name of the form type required, case sensitive
    Returns:
        True if the line represents a form that fits form type required
        False if the line does not
    """
    try:
        norm_type = re.compile(r'[^\w]')
        type_t = re.sub(norm_type,'',chunks[2].strip().lower())
        type_m = re.sub(norm_type,'',form_type.lower())
        if type_m == type_t:
            return True
        else:
            return False
    except:
        logger.error('form type need to be a string')
        return False

def evt_filter(csv_index,evt_csv,evtdate,mperiods):
    """Keep filings for the specific period after a event

    Arguments:
        csv_index {Path} -- The Path for the csv file containing all filings
        evt_csv {Path} -- The Path for the csv file containing the event dates
        evtdate {str} -- The variable name of the event date
        mperiods {int} -- The number of months after the event dates

    Returns:
        Path -- The Path for the resulting csv file
    """
    all_index = pd.read_csv(csv_index,parse_dates=['filing_date'])
    all_index['cik'] = all_index['cik'].apply(str)
    evt = pd.read_csv(evt_csv,parse_dates=[evtdate])
    evt['post_evt'] = evt[evtdate] + pd.DateOffset(months=mperiods)
    evt['pre_evt'] = evt[evtdate] - pd.DateOffset(days=10)
    while True:
        try:
            combined = pd.merge(all_index,evt,left_on='cik',right_on='CIK',how='left')
            break
        except ValueError:
            evt['CIK'] = evt['CIK'].apply(str)
    filt = (combined['filing_date'] >= combined['pre_evt']) & \
    (combined['filing_date'] <= combined['post_evt'])
    results = combined.loc[filt].drop(['CIK',evtdate,'post_evt','pre_evt'],axis=1)
    results.drop_duplicates(keep='first',inplace=True)
    csv_out = csv_index.resolve().parent / f'{csv_index.name[:-4]}_{mperiods}m.csv'
    results.to_csv(csv_out,index=False)
    return csv_out
    




