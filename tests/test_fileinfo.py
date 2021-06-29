import sys
import os
from pathlib import Path
import logging
import re
import pytest
from bs4 import BeautifulSoup


sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import get_edgar.extractor.fileinfo_extractor as finfo_ext


testsfolder = Path(__file__).parent.parent / 'data' / 'tests'
index_csv = testsfolder / 'index_breach_8-K_2018.csv'

finfo_csv = testsfolder / 'info_breach_8-K_2018.csv'

def test_save_finfo():
    finfo = finfo_ext.save_file_info(index_csv,testsfolder)
    assert finfo == finfo_csv

def test_get_isoup():
    page = 'https://www.sec.gov/Archives/edgar/data/1101680/0001101680-15-000011-index.html'
    soup = finfo_ext.get_isoup(page)
    assert soup is not None


def test_filer_info_std():
    record = {"cik":"20","html_index":'https://www.sec.gov/Archives/edgar/data/20/0000893220-08-000688-index.html'}
    soup = finfo_ext.get_isoup(record.get("html_index"))
    filer_info = finfo_ext.get_filer_info(soup,record)
    for k, v in filer_info.items():
        print(f'{k}:{v}')
    assert filer_info.get('busi_add') != None
    assert filer_info.get('mail_add') != None
    assert filer_info.get('busi_phone') != None
    assert len(filer_info) == 10

def test_filer_info_co():
    record = {"cik":"44545","html_index":'https://www.sec.gov/Archives/edgar/data/44545/0000092122-08-000010-index.html'}
    soup = finfo_ext.get_isoup(record.get("html_index"))
    filer_info = finfo_ext.get_filer_info(soup,record)
    for k, v in filer_info.items():
        print(f'{k}:{v}')
    assert filer_info.get('busi_add') != None
    assert filer_info.get('mail_add') != None
    assert filer_info.get('busi_phone') != None
    assert len(filer_info) == 20

def test_filer_info_busim():
    record = {"cik":"101116","html_index":'https://www.sec.gov/Archives/edgar/data/101116/0000950133-08-001362-index.html'}
    soup = finfo_ext.get_isoup(record.get("html_index"))
    filer_info = finfo_ext.get_filer_info(soup,record)
    for k, v in filer_info.items():
        print(f'{k}:{v}')
    assert filer_info.get('busi_add') == ''
    assert filer_info.get('mail_add') != None
    assert filer_info.get('busi_phone') == None
    assert len(filer_info) == 10

def test_filer_info_intl():
    record = {"cik":"63271","html_index":'https://www.sec.gov/Archives/edgar/data/63271/0001193125-11-178201-index.html'}
    soup = finfo_ext.get_isoup(record.get("html_index"))
    filer_info = finfo_ext.get_filer_info(soup,record)
    for k, v in filer_info.items():
        print(f'{k}:{v}')
    assert filer_info.get('busi_add') != None
    assert filer_info.get('mail_add') != None
    assert filer_info.get('busi_phone') != None
    assert len(filer_info) == 10

def test_filer_info_mailm():
    record = {"cik":"845982","html_index":'https://www.sec.gov/Archives/edgar/data/845982/0001193125-11-055004-index.html'}
    soup = finfo_ext.get_isoup(record.get("html_index"))
    filer_info = finfo_ext.get_filer_info(soup,record)
    for k, v in filer_info.items():
        print(f'{k}:{v}')
    assert filer_info.get('busi_add') != None
    assert filer_info.get('mail_add') == ''
    assert filer_info.get('busi_phone') == None
    assert len(filer_info) == 9
