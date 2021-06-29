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
import requests

import get_edgar.common.my_csv as mc
import get_edgar.common.utils as utils

logger = logging.getLogger(__name__)

EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEC_PREFIX = "https://www.sec.gov"


def save_file_info(csv_in,folder):
    """ from csv containing EDGAR records to csv containting EDGAR record and file information
        the new csv file has prefix "info_" to the original csv file name
    
    Arguments:
        csv_in {[Path]} -- [the csv file that contains EDGAR records]
        folder {[Path]} -- [the folder to save the new csv file]

    Returns:
        {[Path]} -- [the path for the csv created]
    """
    csv_save = folder / f'info_{csv_in.name[6:]}'
    if csv_save.exists() == False:
        logger.info(f"start save file info to {csv_in.name}")
        new_rows = get_file_info(csv_in)
        sorted_rows = mc.multikeysort_int(new_rows,'cik','filing_date')
        mc.save_dict_csv(sorted_rows,csv_save)
        logger.info(f"{csv_save.name} created")
    else:
        logger.info(f"{csv_save} already exists")
    return csv_save

## Add file information and htm link to the index downloaded    

def get_file_info(csv_in):
    """ Get form and filer information for all EDGAR records in a csv file
    
    Arguments:
        csv_in {Path} -- the Path object for the csv file that contains EDGAR records,
                         the columns should include "cik", "filing_date", and "html_index"
    
    Returns:
        [list] -- [list of dictionaries containing original EDGAR record, form, and filer information]
    """
    logger.debug(f"start add file info to {csv_in.name}")
    new_rows = []
    with open(csv_in, 'r', newline='') as f:
        reader = csv.DictReader(f)           
        for row in reader:
            time.sleep(1)
            isoup = get_isoup(row.get('html_index'))
            if isoup == None:
                continue 
            links = get_htm_links(isoup, row.get('html_index'))
            for num,link in links:
                row[f'htm_link_{num}'] = link
            form_infos = get_form_info(isoup, row.get('html_index'))
            row.update(form_infos)
            filer_infos = get_filer_info(isoup, row)
            row.update(filer_infos)
            # row['year'] = int(f'{csv_in.name[-8:-4]}')
            new_rows.append(row)
    logger.debug(f"list of records with file info created for {csv_in.name}")
    return new_rows



def get_isoup(page):
    """[Get soup for the EDGAR index page for each EDGAR file]
    
    Arguments:
        page {str} -- [webpage address]
    
    Returns:
        [BeautifulSoup] -- [the soup parsed using BeautifulSoup]
    """
    i = 0
    while True:
        try:
            html_index = requests.get(page, headers=utils.headers)
            break
        except (requests.exceptions.HTTPError):
            logger.error(f'try wait a minute to reopen {page}')
            time.sleep(70)
        except Exception:
            logger.error(f"cannot download {page}", exc_info=True)
            i += 1
            if i > 10:
                return None
            time.sleep(70)
    try:
        return BeautifulSoup(html_index.text, 'lxml')
    except Exception:
        logger.error(f'Cannot make soup for {page}', exc_info=True)
        return None

    
def get_htm_links(soup, index_path):
    """[Get page address for the first webpage from an EDGAR file index page]
    
    Arguments:
        soup {[BeautifulSoup]} -- [the soup for the EDGAR file index page]
        index_path {[str]} -- [the page address for the EDGAR file index page]
    
    Returns:
        [str] -- [the page address for the first webpage in the EDGAR file index page]
    """
    try:
        links = soup.find_all(href=re.compile(r"Archives.*\.htm"))
        page_links = enumerate([SEC_PREFIX + link['href'] for link in links],1)
        return [(n,re.sub(r'/ix\?doc=/','/',link)) for (n,link) in page_links]
    except:
        logger.exception(f'Unable to get htm pages for {index_path}')
        return None


form_headers = {
    "type_description":re.compile(r'Form\s(.*)(?=:)', re.IGNORECASE),
    "report_period":re.compile(r'\sPeriod of Report\s(\d{4}-\d{2}-\d{2})',re.IGNORECASE),
    "file_date":re.compile(r'\sFiling Date\s(\d{4}-\d{2}-\d{2})',re.IGNORECASE),
    "accepted_time":re.compile(r'\sAccepted\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})',re.IGNORECASE),
    "accession_num":re.compile(r'SEC Accession No.\s(\d{10}-\d{2}-\d{6})', re.IGNORECASE),
    "items":re.compile(r'Items\s((Item.*))', re.IGNORECASE),
}

def get_form_info(soup, index_path, headers=form_headers):
    """[Get form information from an EDGAR file index page
        the form information is set through "form_headers"
        ]
    
    Arguments:
        soup {[BeautifulSoup]} -- [the soup for the EDGAR file index page]
        index_path {[str]} -- [the page address for the EDGAR file index page]
    
    Returns:
        [dict] -- [dictionary representing the form information on the EDGAR file index page]
    """
    form_infos = {}
    form_texts = []
    form_texts.append(soup.find(id="formName").get_text())
    form_texts.append(soup.find(id="secNum").get_text())
    formgroup = soup.find_all("div", {"class":"formGrouping"})
    for group in formgroup:
        form_texts.append(group.get_text())
    tems = headers.items()
    for form_text in form_texts:
        for k, v in tems:
            matches = v.search(form_text)
            if matches:
                form_infos[k] = matches.group(1)
    if len(form_infos) == 0:
        logger.warning(f'No form infos for {index_path}')
    elif len(form_infos) < len(headers):
        logger.debug(f'Incomplete form infos for {index_path}')
    return form_infos

filer_headers = {
    "cname":re.compile(r'\n(.+)\(Filer\)', re.IGNORECASE),
    "fcik":re.compile(r'CIK:\s(\d{10})', re.IGNORECASE),
    "sic":re.compile(r'SIC:\s(\d{4})', re.IGNORECASE),
    "irs_num":re.compile(r'IRS No.:\s(\d+)\s', re.IGNORECASE),
    "year_end":re.compile(r'Fiscal Year End:\s(\d{4})', re.IGNORECASE),
    "state_incorp":re.compile(r'State of Incorp.:\s(\w{2})', re.IGNORECASE),
    "type":re.compile(r'Type:\s(.+?)\s(?=\|)', re.IGNORECASE)
}

fcik_pattern = filer_headers.get("fcik")
fcname_pattern = filer_headers.get("cname")

def get_filer_info(soup, record):
    """[Get filer information from an EDGAR file index page
        the filer information is set through "filer_headers"
        the filer cik needs to be the same as cik in the EDGAR file record]
    
    Arguments:
        soup {[BeautifulSoup]} -- [the soup for the EDGAR file index page]
        record {[dict]} -- [dictionary representing the EDGAR file record]
    
    Returns:
        [dict] -- [dictionary representing the filer information on the EDGAR file index page]
    """
    filer_infos = {}
    co_filers_cik = []
    co_filers_cname = []
    all_filer_texts = soup.find_all("div",id="filerDiv")
    tems = filer_headers.items()
    for filer_texts in all_filer_texts:
        filer_text = filer_texts.get_text()
        fcik_info = fcik_pattern.search(filer_text)
        if fcik_info:
            fcik = fcik_info.group(1)
            if str(int(fcik)) != record.get("cik"):
                logger.debug(f'found co-filer in {record.get("html_index")}')
                co_filers_cik.append(fcik)
                cname_info = fcname_pattern.search(filer_text)
                if cname_info:
                    cname = cname_info.group(1)
                    co_filers_cname.append(cname.strip())
                continue
        for k, v in tems:
            matches = v.search(filer_text)
            if matches:
                info_raw = matches.group(1)
                info = info_raw.replace('\n', ' ').replace('\r', ' ')
                filer_infos[k] = info.strip()
        filer_infos = {**filer_infos, **extract_address(filer_texts)}
    if len(filer_infos) == 0:
        logger.warning(f'No filer infos for {record.get("html_index")}')
    if co_filers_cik:
        for i in range(len(co_filers_cik)):
            filer_infos[f'co_filers_cik_{i}'] = co_filers_cik[i]
            try:
                filer_infos[f'co_filers_cname_{i}'] = co_filers_cname[i]
            except IndexError:
                filer_infos[f'co_filers_cname_{i}'] = None
    return filer_infos


def select_items(csv_in,filters):
    csv_out = csv_in.resolve().parent / f'item_{csv_in.name[5:]}'
    if csv_out.exists() == False:
        mc.text_filter(csv_in,csv_out,'items',filters)
        logger.info(f'{csv_out} created')
    else:
        logger.info(f'{csv_out} already exists')

def extract_address(filer_info):
    addresses = filer_info.find_all('div','mailer')
    if addresses:
        for address in addresses:
            add_des = address.contents[0].strip()
            mailer_address = []
            phone = None
            adds = address.find_all('span',class_='mailerAddress')
            if adds:    
                for add in adds:
                    add_text = add.get_text().strip()
                    if any(c.isalpha() for c in add_text):
                        mailer_address.append(add_text)
                    else:
                        phone = ''.join([c for c in add_text if c.isdigit()])
            if add_des == 'Mailing Address':
                mail_add = ','.join(mailer_address)
            elif add_des == 'Business Address':
                busi_add = ','.join(mailer_address)
                busi_phone = phone
        return {'mail_add':mail_add, 'busi_add':busi_add,'busi_phone':busi_phone}
    return {'mail_add':None, 'busi_add':None,'busi_phone':None}
    
