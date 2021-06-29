# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 09:28:47 2019

@author: Lin
"""

import csv
import sys
import math
from pathlib import Path
import logging
from operator import itemgetter
import pandas as pd

maxInt = min(sys.maxsize, 2147483646)

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/2)

logger = logging.getLogger(__name__)

def save_dict_csv(dict_list_tosave, csv_save):
    """[Save a list of dictionaries to csv file]
    
    Arguments:
        dict_list_tosave {[list]} -- [list of dicts to save to csv_save]
        csv_save {[Path]} -- [the Path object of the csv file]
    """
    all_keys = [list(dicts.keys()) for dicts in dict_list_tosave]
    all_key = [item for sublist in all_keys for item in sublist]
    columns = list(set(all_key))
    columns.sort()
    try:
        with open(csv_save, 'w', newline='') as f:
            csv_out_writer = csv.DictWriter(f,fieldnames=columns,restval='',extrasaction='ignore')
            csv_out_writer.writeheader()
            for item in dict_list_tosave:
                csv_out_writer.writerow(item)        
    except UnicodeEncodeError:
        logger.error("encoding error")
        try:
            with open(csv_save, 'w', newline='',encoding='utf-8-sig') as f:
                csv_out_writer = csv.DictWriter(f,fieldnames=columns,restval='',extrasaction='ignore')
                csv_out_writer.writeheader()
                for item in dict_list_tosave:
                    csv_out_writer.writerow(item)
        except UnicodeEncodeError:
            logger.exception("encoding error utf-8-sig")
        else:
            logger.debug(f'{csv_save} finished with utf-8-sig')    
    else:  
        logger.debug(f'{csv_save} finished')

def split_csv_size(csv_file, size, folder, keepheader=True):
    """[Split a large csv file into small csv files with certain rows in each small csv file]
    
    Arguments:
        csv_file {[Path]} -- [the Path object for the large csv file to split]
        size {[int]} -- [number of observations in each small csv file]
        folder {[Path]} -- [the folder to save all the small csv files]
    
    Keyword Arguments:
        keepheader {bool} -- [whether to keep the header row in the small csv files] 
        (default: {True})
    
    Returns:
        [list] -- [list of Path for all the small csv files generated]
    """
    csv_name = csv_file.name[:-4]
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        current_piece = 1
        current_out = folder / f'{csv_name}_{current_piece}.csv'
        current_out_writer = csv.writer(open(current_out, 'w', newline=''))
        if keepheader == True:
            current_out_writer.writerow(header)
        current_limit = size
        for i, row in enumerate(reader):
            if i + 1 > current_limit:
                current_piece += 1
                current_limit = size * current_piece
                current_out = folder / f'{csv_name}_{current_piece}.csv'
                current_out_writer = csv.writer(open(current_out, 'w', newline=''))
                if keepheader == True:
                    current_out_writer.writerow(header)
            current_out_writer.writerow(row)
        outfile = [f'{csv_name}_{i}.csv' for i in range(1,current_piece+1)]
    return sorted([folder / outfile[i] for i in range(current_piece)])

def split_csv_num(csv_file, num, folder, keepheader=True):
    """[Split a large csv file into certain number of small csv files with equal size]
    
    Arguments:
        csv_file {[Path]} -- [the Path object for the large csv file to split]
        num {[int]} -- [the number of small csv files to create]
        folder {[Path]} -- [the folder to save all the small csv files]
    
    Keyword Arguments:
        keepheader {bool} -- [whether to keep the header row in each small csv file] 
        (default: {True})
    
    Returns:
        [list] -- [list of Path for all the small csv files generated]
    """
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        next(reader)
        row_count = sum(1 for row in reader)
        size = math.ceil(row_count/num)
    return split_csv_size(csv_file, size, folder, keepheader=keepheader)

def merge_csv(csv_list, newname, folder,hasheader=True,save=True):
    """[merge multiple csv files, 
        the new csv file name is according to the first file in the list]
    
    Arguments:
        csv_list {list} -- [the list of Path for all the small csv files to merge]
        newname {string} -- [the name for the merged csv file]
        folder {[Path]} -- [the folder to save the merged csv file]
    
    Keyword Arguments:
        hasheader {bool} -- [whether there is header in each small csv file] 
        (default: {True})
    
    Returns:
        [Path] -- [the Path object for the merged csv file]
    """
    all_obs = []
    for file in csv_list:
        try:
            file_obs = []
            with open(file, newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    file_obs.append(row)
        except UnicodeDecodeError:
            logger.error("decoding error")
            try:
                file_obs = []
                with open(file, newline='',encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        file_obs.append(row)
            except UnicodeDecodeError:
                logger.exception("decoding error with utf-8-sig")
            else:
                all_obs += file_obs
        else:
            all_obs += file_obs
    if save:
        csv_merge = folder / (newname + '.csv')
        save_dict_csv(all_obs,csv_merge)
        return csv_merge
    else:
        return all_obs

def extract_obs(csv_in,var,dup=False,blank=False):
    """Extract observations for a variable 

    Arguments:
        csv_in {Path} -- csv file to extract observations
        var {Str} -- variable name, case sensitive

    Keyword Arguments:
        dup {bool} -- whether to keep duplicate observations (default: {False})
        blank {bool} -- whether to keep null observations (default: {False})

    Returns:
        [type] -- [description]
    """
    with open(csv_in,'r') as cf:
        reader = csv.DictReader(cf)
        if dup:
            if blank:
                return tuple([r.get(var) for r in reader])
            else:
                return tuple([r.get(var) for r in reader if r.get(var).strip()])
        else:
            if blank:
                return tuple(sorted(set([r.get(var) for r in reader])))
            else:
                return tuple(sorted(set([r.get(var) for r in reader if r.get(var).strip()]))) 

def text_filter(csv_in,csv_out,var,filters):
    """Select specific observations according the value of a var

    Arguments:
        csv_in {Path} -- unfiltered csv
        csv_out {Path} -- filtered csv
        var {str} -- the filtered variable, case sensitive
        filters {set} -- key words to filter observations
    """
    with open(csv_in,'r',encoding='utf-8') as fin:
        reader = csv.DictReader(fin)
        columns = reader.fieldnames
        if var in columns:
            with open(csv_out,'w',newline='') as fout:
                writer = csv.DictWriter(fout,fieldnames=columns,restval='',extrasaction='ignore')
                writer.writeheader()
                for row in reader:
                    if row.get(var): 
                        for filter in filters:
                            if filter in row.get(var):
                                writer.writerow(row)
                                break
        else:
            logger.warning(f'{csv_in} does not have {var}')


def multikeysort_int(items_unsort,*keys,intkeys=None):
    if intkeys is not None:
        for item in items_unsort:
            item.update((k,int(v)) for k, v in item.items() if k in intkeys)
    return sorted(items_unsort,key=itemgetter(*keys))


def sort_by_columns(csv_in,csv_out,*keys,intkeys=None):
    try:
        with open(csv_in,newline='') as csvfile:
            reader = list(csv.DictReader(csvfile))
            sortedlist = multikeysort_int(reader,*keys,intkeys=intkeys)
    except UnicodeDecodeError:
        logger.error("decoding error")
        try:
            with open(csv_in,newline='',encoding='utf-8-sig') as csvfile:
                reader = list(csv.DictReader(csvfile))
                sortedlist = multikeysort_int(reader,*keys,intkeys=intkeys)
        except UnicodeDecodeError:
            logger.exception("decoding error with utf-8-sig")
        else:
            save_dict_csv(sortedlist,csv_out)
    else:
        save_dict_csv(sortedlist,csv_out)



def read_file_df(csv_in,**readkwargs):
    if csv_in.suffix == '.csv':
        for encode in ('cp1252','utf-8-sig'):
            try:
                df = pd.read_csv(csv_in,encoding=encode,**readkwargs)
                return df
            except UnicodeDecodeError:
                continue
    elif csv_in.suffix == '.xlsx':
        try:
            df = pd.read_excel(csv_in,**readkwargs)
            return df
        except UnicodeDecodeError:
            logger.exception('cannot read excel')


def to_file_df(df, csv_out,**savekwargs):
    for encode in ('cp1252','utf-8-sig'):
        if csv_out.suffix == '.csv':
            try:
                df.to_csv(csv_out,encoding=encode,index=False,**savekwargs)
                break
            except UnicodeEncodeError:
                continue
        elif csv_out.suffix == '.xlsx':
            try:
                df.to_excel(csv_out,encoding=encode,index=False,**savekwargs)
                break
            except UnicodeEncodeError:
                continue
    return csv_out
