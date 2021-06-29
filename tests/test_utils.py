import sys
import os
from pathlib import Path
import pytest
import chardet
import pandas as pd
import numpy as np

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_edgar.common.utils import clean_cname,csv_clean_cname,csv_getencoding,json_to_csv,match_cname,\
    gen_ngrams, search_grams, search_grams_in_series,gen_2grams, find_specialcharacter,\
        special_in_series, grams_len, grams_len_max,match_grade,add_fyear

from get_edgar.common.my_csv import to_file_df, read_file_df

datafolder = Path(__file__).parent.parent / 'data'

def test_cleancname():
    assert clean_cname('TripAdvisor, Inc') == 'TripAdvisor'
    assert clean_cname('Dun & Bradstreet Corp/NW') == 'Dun & Bradstreet'
    assert clean_cname('CVS Health Corp') == 'CVS Health'
    assert clean_cname('CHARTER COMMUNICATIONS, INC. /MO/') == 'CHARTER COMMUNICATIONS'
    assert clean_cname('Mercantil Servicios Financieros, C.A.') == 'Mercantil Servicios Financieros'

breach_cname = datafolder / 'breach_cname.csv'
breach_ccname = datafolder / 'breach_ccname.csv'

cname_vars = ('cname','tcname')
cname_ticker = datafolder / 'cname_cik_ticker.csv'
cname_ticker_clean = datafolder / 'cname_ticker_clean.csv'
cname_cik_clean = datafolder / 'cname_cik.csv'


def test_csv_ccname():
    csv_clean_cname(breach_cname,breach_ccname,cname_vars)
    csv_clean_cname(cname_ticker,cname_ticker_clean,('title',))


def test_csv_getencoding():
    encode = csv_getencoding(breach_cname)
    assert encode == 'cp1252'

test_json = datafolder / 'company_tickers.json'
test_company_ticker = datafolder / 'cname_cik_ticker.csv'

def test_jsontocsv():
    json_to_csv(test_json,test_company_ticker)

extradatafolder = Path(__file__).parents[2] / 'RavenPack' / 'data'
# breach_cname_clean = extradatafolder / 'breach_cname_clean.csv'
breach_cname_clean = datafolder / 'breach_cname_clean.csv'





# df_nst = pd.read_csv(breach_cname_clean,nrows=40)
df_nst = pd.read_csv(breach_cname_clean)
# df_st = pd.read_csv(cname_ticker_clean,nrows=10000)
df_ticker = pd.read_csv(cname_ticker_clean)
df_cik = read_file_df(cname_cik_clean)


exact_match = datafolder / 'breach_ticker_exactmatch.csv'
bg_match = datafolder / 'breach_ticker_bgmatch.csv'
bg_nonmatch = datafolder / 'breach_ticker_bgnonmatch.csv'

def test_match_cname(df_st):
    ematched, nematched = match_cname(df_nst,df_st,'clean_cname','title_clean',cri='exact')
    (a,_) = ematched.shape
    (c,_) = nematched.shape
    assert a > 1
    assert c > 1
    ematched.to_csv(exact_match,index=False)
    bgmatched, nbgmatched = match_cname(nematched,df_st,'clean_cname','title_clean',cri='2-grams')
    (b,_) = bgmatched.shape
    (d,_) = nbgmatched.shape
    assert b > 1
    assert d > 1
    bgmatched.to_csv(bg_match,index=False)
    nbgmatched.to_csv(bg_nonmatch,index=False)


def test_genngrams():
    bigrams_1 = gen_ngrams('21st century oncology holdings')
    bigrams_2 = gen_ngrams('adp (us bankcorp)')
    bigrams_3 = gen_ngrams('adp')
    bigrams_4 = gen_ngrams('')
    assert bigrams_1 == ['21st century', 'century oncology', 'oncology holdings']
    assert bigrams_2 == ['adp (us', '(us bankcorp)']
    assert bigrams_3 == ['adp']
    assert bigrams_4 == []

def test_gen2grams():
    bigrams_1 = gen_2grams('21st century oncology holdings')
    bigrams_2 = gen_2grams('adp us bankcorp')
    bigrams_3 = gen_2grams('t mobile')
    bigrams_4 = gen_2grams('')
    assert bigrams_1 == [{'21st century oncology holdings'},['21st century oncology', 'century oncology holdings']]
    assert bigrams_2 == [{'adp us bankcorp'}, {'adp us', 'us bankcorp'},['adp']]
    assert bigrams_3 == [{'t mobile','mobile'}]
    assert bigrams_4 == None

@pytest.mark.parametrize("df_st",[df_ticker,df_cik])
def test_searchgramsinseries(df_st):
    cname_st_series = df_st['title_clean'].str.lower()

    def search_sample(text,assertion):
        text = text.lower()
        bigrams = gen_2grams(text)
        index_test = search_grams_in_series(bigrams,cname_st_series)
        print(index_test)
        if assertion == True:
            assert index_test != []
        else:
            assert index_test == []
    
    search_sample('21st century oncology holdings',True)
    search_sample('adobe systems',True)
    search_sample('acme',True)
    search_sample('6 hotels',False)
    search_sample('alaska communications',True)
    search_sample('t mobile',True)
    search_sample('banco Santander Mexico , institucion de Banca Multiple, Grupo Financiero Santander Mexico',True)
    search_sample("Casey's General Stores",True)
    search_sample("Whole Foods",True)


def test_findspecial(df_st):
    special_1 = find_specialcharacter("ADAMS RESOURCES & ENERGY")
    assert special_1 == ['&']
    allspecial, _ = special_in_series(df_st['title_clean'])
    assert len(allspecial) > 1


def test_gramslen():
    len_1 = grams_len("ADAMS RESOURCES & ENERGY")
    len_2 = grams_len("Banco Santander Mexico , Institucion de Banca Multiple, Grupo Financiero Santander Mexico")
    len_3 = grams_len("ADAMS RESOURCES & ENERGY,")
    len_4 = grams_len("PG&E")
    len_5 = grams_len("Macy's")
    assert len_1 == 3
    assert len_2 == 4
    assert len_3 == 3
    assert len_4 == 1
    assert len_5 == 1

def test_gramlenmax(df_st):
    lm_1 = grams_len_max(df_st['title_clean'])
    lm_2 = grams_len_max(df_nst['cname_clean'])
    assert lm_1 == 10
    assert lm_2 == 4

def test_matchgrade():
    bgmatch = pd.read_csv(bg_match)
    bgmatch['match_grade'] = bgmatch.apply(lambda df: match_grade(df.match_gram,df.title_clean),axis=1)
    # hgrade = bg_match

def test_fyear():
    report_period_1 = '2014-01-10' 
    report_period_2 = '2014-10-10' 
    year_end_1 = 1231
    year_end_2 = 102
    year_end_3 = 831
    year_end_4 = np.nan
    fyear_1 = add_fyear(report_period_1,year_end_1,annual=False)
    assert fyear_1 == 2013
    fyear_2 = add_fyear(report_period_1,year_end_2,annual=False)
    assert fyear_2 == 2013
    fyear_3 = add_fyear(report_period_2,year_end_2,annual=False)
    assert fyear_3 == 2014
    fyear_4 = add_fyear(report_period_1,year_end_3,annual=False)
    assert fyear_4 == 2014
    fyear_5 = add_fyear(report_period_2,year_end_3,annual=False)
    assert fyear_5 == 2015
    fyear_6 = add_fyear(report_period_1,year_end_4,annual=True)
    assert fyear_6 == 2013
    
