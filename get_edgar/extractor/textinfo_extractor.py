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
from bs4 import BeautifulSoup

import get_edgar.common.my_csv as mc
import get_edgar.postprocessor.text_analysis as text_ana


logger = logging.getLogger(__name__)

# retrieve each file webpage address from each row in the info csv file
def get_filepage(info_row):
    """Retrieve all file page associate with a specific file

    Arguments:
        info_row {dict} -- the dictionary containing the file links

    Returns:
        list of set -- [(num,link) for all links in the dictionary provided]
    """
    return [(s[9:],info_row[s]) for s in info_row if s.startswith('htm_link')]

def get_info_row(csv_in):
    """Generate list of dictionarys for each row in the csv

    Arguments:
        csv_in {Path} -- the csv containing all the file info

    Returns:
        list of dict -- all rows in dicts
    """
    with open(csv_in, 'r', newline='') as f:
        reader = list(csv.DictReader(f))
        cfkeys = ('cik','filing_date')
        sorted_r = mc.multikeysort_int(reader,*cfkeys,intkeys=('cik'))
        return [row for row in sorted_r]

def add_textanalysis(info_row,suffix,models=None,**kwargs):
    """Add text analysis results to the info dict

    Arguments:
        info_row {dict} -- the dict containing file info
        filters {set} -- the set containing all the key words needed
        excludes {set} -- the set containing key words not needed
        suffix {str} -- the suffix for the new variables to represent the filters

    Returns:
        dict -- new dict containing all file info & text info
    """
  
    file_head = ['---------- Filings Start ----------\n', \
                f"cik : {info_row.get('cik')}\n", \
                f"company name : {info_row.get('conm')}\n", \
                f"form type : {info_row.get('form_type')}\n", \
                f"filing date : {info_row.get('filing_date')}\n\n"]
    linkn = 0
    wfilter = 0
    filtered_para = []
    irr = []
    imgs = []
    filtered_text = []
    filtered_title = []
    for num, link in get_filepage(info_row):
        if link:
            filtered_para.append(f'\nFiling # {num}\n\n')
            filtered_para.append(f'{link}\n')
            linkn += 1
            page = text_ana.text_page(link)
            if page.parags is None:
                try:
                    if page.irr_parags:
                        irr.append(num)
                except AttributeError:
                    imgs.append(num)
                filtered_para.append(f'Cannot extract paragraphs\n\n')
                continue
            else:
                section_fls = page.section_slice(filters=('FORWARD-LOOKING','SAFE HARBOR'))
                filtered = page.filtered_parags(section_exc=section_fls,joined=True,**kwargs)
            if filtered:
                wfilter += 1
                filtered_para = filtered_para + [para.text for para in filtered]
                filtered_text = filtered_text + [para.text for para in filtered if (not para.istitle)]
                filtered_title = filtered_title + [para.text for para in filtered if para.istitle]
                filtered_para.append('\n\n')

            info_row[f'has_parag_{suffix}_{num}'] = bool(filtered)
    info_row['irr_parags'] = irr
    info_row['img_parags'] = imgs
    info_row[f'has_parag_{suffix}'] = wfilter
    if filtered_text:
        filtered_t = ' '.join(filtered_text).strip()
        info_row['filtered_text'] = filtered_t
    if filtered_title:
        info_row['filtered_title'] = filtered_title
    file_end = [f'\n\nTotal files available: {linkn}\n', \
                f'files with {suffix} : {wfilter}\n', \
                '---------- Filings End ----------\n\n']
    info_txt = file_head + filtered_para + file_end
    return info_row, info_txt, wfilter

def save_textanalysis(csv_in,folder,save_txt=True,**kwargs):
    """Save text analysis results to a new csv

    Arguments:
        csv_in {Path} -- csv with file info (links to files)
        folder {Path} -- folder to save the results
    """
    csv_out = folder / f'{csv_in.name[5:]}'
    txt_folder = folder.resolve().parent / 'text' 
    if txt_folder.exists() == False:
        txt_folder.mkdir()
    txt_out_all = txt_folder / f'all_{csv_out.name[:-4]}.txt'
    txt_out_select = txt_folder / f'select_{csv_out.name[:-4]}.txt'
    if csv_out.exists() == False:
        all_info = [add_textanalysis(r,**kwargs) for r in get_info_row(csv_in)]
        # for r in get_info_row(csv_in):
        #     nr,txt = add_textanalysis(r,**kwargs)
        if csv_out.exists() == False:
            nr = [row for (row,itext,wfn) in all_info]
            mc.save_dict_csv(nr,csv_out)
            logger.info(f'{csv_out.name} created')
        else:
            logger.info(f'{csv_out.name} already exists')
        if txt_out_all.exists() == False:
            if save_txt:
                with open(txt_out_all,'a',encoding='utf-8') as f:
                    for p in [itext for (row,itext,wfn) in all_info]:
                        f.writelines(p)
                logger.info(f'{txt_out_all.name} created')
                
                with open(txt_out_select,'a',encoding='utf-8') as f:
                    for p in [itext for (row,itext,wfn) in all_info if wfn]:
                        f.writelines(p)
                logger.info(f'{txt_out_select.name} created')
            else:
                logger.info('no need to save txt file')
        else:
            logger.info(f'{txt_out_all.name} already exists')
        del all_info
    else:
        logger.info(f'{csv_out.name} & {txt_out_all.name} already exist')
    return csv_out

       

