import sys
import os
from pathlib import Path
import pytest

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from get_edgar.common import my_csv as mc
# from get_edgar.downloader import indexdownloader as ind_dl
# from get_edgar.downloader import filedownloader as f_dl
from get_edgar.extractor import fileinfo_extractor as finfo_ext 
from get_edgar.extractor import textinfo_extractor as tinfo_ext
# from get_edgar.postprocessor import text_analysis as text_ana
import get_edgar.options as opts

from get_edgar.common.utils import split_n

testsfolder = Path(__file__).parent.parent / 'data' / 'tests'
datafolder = Path(__file__).parent.parent / 'data'


index_csv = testsfolder / 'index_breach_8-K_2018.csv'

finfo_csv = testsfolder / 'info_breach_8-K_2018.csv'

tinfo_csv = testsfolder / 'breach_8-K_2018.csv'


def test_readfile():
    with pytest.raises (SystemExit) as excinfo:
        filepath = Path(testsfolder,'breach_dict.txt')
        opts.read_filts(filepath)
        assert excinfo.value == f'{filepath} not valid'

cfkeys = ('cik','filing_date')

def test_split_n():
    args = {'save_txt':True, 'suffix':'breach', 'filters':'cyber', 'excludes':'forward-looking'}

    @split_n(2,testsfolder,sort=True,sortkeys=cfkeys,intkeys=('cik'))
    def main_fileinfo(csv_in,folder):
        return finfo_ext.save_file_info(csv_in,folder)

    @split_n(2,testsfolder,sort=True,sortkeys=cfkeys,intkeys=('cik'))
    def main_textinfo(csv_in,folder,**kwargs):
        return tinfo_ext.save_textanalysis(csv_in,folder,**kwargs)

    finfo = main_fileinfo(index_csv,testsfolder)

    assert finfo == finfo_csv
    assert main_textinfo(finfo_csv,testsfolder,**args) == tinfo_csv



filter_path_1 = datafolder / 'breach_dict.xlsx'
filter_path_2 = datafolder / 'AC_dict.xlsx'

@pytest.mark.parametrize("filepath",[filter_path_1,filter_path_2])
def test_read_filts(filepath):
    filters = opts.read_filts(filepath)
    for filter in filters:
        print(filter)
        print(type(filter))
    assert len(filters) > 0
