import sys
import os
from pathlib import Path
import logging
import pytest

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_edgar.common.my_csv import extract_obs, read_file_df, to_file_df

datafolder = Path(__file__).parent.parent / 'data'

breach_cname = datafolder / 'breach_cname.csv'

def test_extract_obs():
    breach_dates = extract_obs(breach_cname,'disclosure_date')
    assert isinstance(breach_dates,tuple)

def test_read_fild_df():
    csv_file = datafolder / 'tests' / 'tinfo_20-F.csv'
    xlsx_file = datafolder / 'tests' / 'tinfo_20-F.xlsx'
    assert csv_file.exists()
    assert xlsx_file.exists()
    df_csv = read_file_df(csv_file)
    assert df_csv.shape[0] > 0
    df_xlsx = read_file_df(xlsx_file)
    assert df_xlsx.shape[0] > 0
