import sys
import os
import pytest

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_edgar.downloader.cik_complete import EDGAR_Index

@pytest.fixture
def index_sample():
    year = 2020
    return EDGAR_Index(year)

def test_extract_table(index_sample):
    index_table = index_sample.extract_table(0) 
    assert list(index_table.columns) == [
        'CIK',
        'Company Name',
        'Form Type',
        'Date Filed',
        'Filename'
    ]
    print(index_table.head())
    print(index_table.dtypes)

def test_cik_cname_id(index_sample):
    df_all = index_sample.cik_cname_id()
    assert list(df_all.columns) == [
        'CIK',
        'Company Name',
        'Form Type',
        'Date Filed',
        'Filename',
        'company_p',
        'new_company',
        'companyid'
    ]

def test_extract_period(index_sample):
    index_period = index_sample.extract_period()
    assert index_period.index.names == [
        'CIK',
        'Company Name'
    ]
    assert list(index_period.columns) == [
        ('Date Filed','amin'),
        ('Date Filed','amax'),
        ('Filename', 'size')
    ]
    assert index_period.index.has_duplicates == True
