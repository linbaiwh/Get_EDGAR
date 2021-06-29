#%%
import csv
from pathlib import Path
import logging
import re
import pandas as pd
import numpy as np
from multiprocessing import Pool
import multiprocessing
from functools import partial
from datetime import datetime, date, timedelta
import calendar
import math
import matplotlib.pyplot as plt
from functools import wraps
from multiprocessing.dummy import Pool as ThreadPool

import get_edgar.common.my_csv as mc


logger = logging.getLogger(__name__)

# import get_edgar.common.my_csv as mc 

def merge_txt(txts_in,txt_out):
    with open(txt_out,'w',encoding='utf-8') as fout:
        for txt_in in txts_in:
            with open(txt_in,'r',encoding='utf-8') as fin:
                fout.write(fin.read())

def clean_cname(cname):
    cname = re.sub(r'(/[A-Z]+/?)','',cname).strip()
    cname = re.sub(r'(\\[A-Z]+\\?)','',cname).strip()
    cname = re.sub(r',?\s?\b(Corp|L\.?L\.?C\.?|LTD|CO|Inc|PLC|L\.?\s?P\.?|S\.?A\.?B?\.?|C\.?V\.?)\b',\
        '',cname,flags=re.I).strip()
    cname = re.sub(r',?\s?\bC\.A\.','',cname).strip()
    # cname = re.sub(r'(,?\s?\bCorp)|(,?\s?LLC\b)|(,?\s?\bLTD\b)|(\bCO\b)|(,?\s?Inc\.?)|(\bPLC\b)|(,?\s?\bL\.?P\.?\b)', \
    #     '',cname,flags=re.I).strip()
    cname = re.sub(r'\(|\)|\.|/|\\|-',' ',cname)
    cname = re.sub(r'\s+',' ',cname).strip()
    return cname


def csv_clean_cname(csv_in,csv_out,cname_vars):
    try:
        if csv_in.suffix == ".csv":
            cname = pd.read_csv(csv_in)
        elif csv_in.suffix == ".xlsx":
            cname = pd.read_excel(csv_in)
    except UnicodeDecodeError:
        encode = csv_getencoding(csv_in)
        try:
            cname = pd.read_csv(csv_in,encoding=encode)
        except UnicodeDecodeError:
            logger.exception('cannot guess encoding')
            return None
    for var in cname_vars:
        cname[var] = cname[var].fillna('')
        cname[f'{var}_clean'] = cname[var].apply(clean_cname)
    if csv_out.suffix == '.csv':
        cname.to_csv(csv_out,index=False,encoding='utf-8')
    elif csv_out.suffix == '.xlsx':
        cname.to_excel(csv_out,index=False)
    return csv_out 

def csv_getencoding(csv_in):
    with open(csv_in,'r') as f:
        return f.encoding
        # return chardet.detect(f.read()).get("encoding")

def json_to_csv(json_in,csv_out):
    df = pd.read_json(json_in,orient='index')
    df.to_csv(csv_out,index=False)
    return csv_out

def find_specialcharacter(text):
    if isinstance(text,str):
        special = re.search(r"([^\s\w])",text)
        if special:
            return [match for match in special.groups()]
    return None

def special_in_series(pdseries):
    allr = pdseries.apply(find_specialcharacter)
    allspecial = set()
    special_index = []
    for index, special in allr.items():
        if special:
            new_sp = set(special)
            if new_sp.difference(allspecial):
                special_index.append(index)
            allspecial = allspecial.union(new_sp)
    if allspecial:
        special_cases = [pdseries[i] for i in special_index]
        return allspecial,special_cases
    else:
        return None

idvars_nst = ['cname','tcname','disclosure_date','cname_clean','tcname_clean','CIK']


def match_cname(df_nst,df_st,cname_nst,cname_st,cri='exact',idvars_nst=idvars_nst):
    df_nst[cname_nst]=  df_nst[cname_nst].str.lower()
    df_st[cname_st]=  df_st[cname_st].str.lower()

    if cri == 'exact':
        mergedl = df_nst.merge(df_st,how='left',left_on=cname_nst,right_on=cname_st,indicator=True)
    elif cri == '2-grams':
        lmax = grams_len_max(df_st[cname_st])
        df_nst[cname_nst] = df_nst[cname_nst].fillna('')
        df_nst['cname_2grams'] = df_nst[cname_nst].apply(gen_2grams,max=lmax)
        matches_i = df_nst['cname_2grams'].apply(search_grams_in_series,args=(df_st[cname_st],))
        data = [(index, *item) for index, item in matches_i.items()]
        series_m = pd.DataFrame(data).set_index(0).stack().droplevel(1)
        matches = pd.DataFrame(series_m.to_list(),columns=['match_index','match_gram'],index=series_m.index)
        matches['match_index'] = matches['match_index'].convert_dtypes()
        matches['nst_index'] = matches.index
        df_nst['nst_index'] = df_nst.index

        merged_1 = pd.merge(df_nst[['nst_index',cname_nst]+idvars_nst],matches,how="left",on='nst_index')
        merged_1['match_index'].fillna(value=-1,inplace=True)
        merged_1['match_gram'].fillna(value='',inplace=True)
        mergedl = merged_1.merge(df_st,how='left',left_on='match_index',right_index=True,indicator=True)

    else:
        pass
    
    matched = mergedl.loc[mergedl['_merge']=='both'].drop(columns='_merge').drop_duplicates()
    nonmatched = mergedl.loc[mergedl['_merge']=='left_only'].drop(columns='_merge')
    return matched, nonmatched

def gen_ngrams(text,n=2):
    words = gen_gram(text,letter=True)
    if len(words) >= n:
        return join_grams(words,n)
    elif (len(words) < n) & (text != ''):
        return [text]
    else:
        return []

def join_grams(grams,n):
    return [" ".join(grams[i:i+n]) for i in range(len(grams)-(n-1))]

def gen_2grams(text,max=5):
    if isinstance(text,str):
        words = gen_gram(text)
        word_letter = gen_gram(text,letter=True)
        len_w = len(words)

        joined_full = " ".join(word_letter)
        joined_words = " ".join(words)

        if len_w > 4:
            if len_w <= max:
                joined_4 = join_grams(words,max-1)
            elif len_w > max:
                joined_4 = join_grams(words,len_w-1)
            
            joined_3 = join_grams(words,3)
            
            return [set([joined_full,joined_words]),joined_4,joined_3]
        
        elif len_w == 4:
            joined_3 = join_grams(words,3)

            return [set([joined_full,joined_words]),joined_3]
        
        elif len_w == 3:    
            joined_2 = set(join_grams(word_letter,2) + join_grams(words,2))
            return [set([joined_full,joined_words]),joined_2,[words[0]]]

        elif len_w == 2:
            joined_2 = set(join_grams(word_letter,2) + join_grams(words,2))
            return [[joined_full],joined_2,[words[0]]]

        elif (len_w == 1) & (text != ''):
            return [set([joined_full,joined_words])]

    return None

stop_gram_prefix = ["financial","capital","global","international","national",\
    "consumer","information","hotel","insurance","interactive","digital","energy","b",\
        "asset","service","group"]
stop_gram_suffix = ["group","service","services","bancorp","company","product","products",\
    "systems","system","corporate","association","media","trust","v","management","public"]

stop_gram_prod = [" ".join([p,s]) for p in stop_gram_prefix for s in stop_gram_suffix]

stop_gram_address = ["hong kong","chicago","illinois","uk","us","american","baltimore",\
    "british","washington","west","italian","china"]
stop_gram_unique = ["and","banco","bankers","big","blue",\
    "casino","casinos","concrete","continental","control","controls","cross border",\
    "data","dairy","dollar","du","e",'element',"factor","family","food","holiday","hotel","hotels",\
        "jewelry","mart","mexican grill","mobile","natural",\
    "other","others","pizza","precision","post","resturants","resorts",\
        "st","the","thomas","time","total","united","unknown",\
            "web"]

all_stop_grams = stop_gram_prefix + stop_gram_suffix + stop_gram_prod + stop_gram_address + \
    stop_gram_unique

def search_grams(grams,text):
    if isinstance(text,str):
        for gram in grams:
            if (gram not in all_stop_grams) & (gram.isdigit() == False):
                gram_w = ' ' + gram + ' '
                text_w = ' ' + text + ' '
                if gram_w in text_w:
                    return gram
    return False

def search_grams_in_series(grams,pdseries):
    result = []
    if grams:
        for gram_i in grams:
            for index, text in pdseries.items():
                match_gram = search_grams(gram_i,text)
                if match_gram:
                    result.append((index,match_gram))
            if result:
                return result
    return result


def grams_len(text):
    l = 0
    if isinstance(text,str):
        bows = text.split(',')
        for bow in bows:
            ln = len(gen_gram(bow))
            if ln > l:
                l = ln
    return l

def gen_gram(text,letter=False):
    words = re.split(r'\s',text)
    if letter == False:
        return [gram for gram in words if (gram != '') & (gram != '&') & (len(gram) > 1)]
    else:
        return [gram for gram in words if (gram != '') & (gram != '&')]

def grams_len_max(pdseries):
    s = pdseries.apply(grams_len)
    return s.max()

def match_grade(match,text):
    return grams_len(match)/grams_len(text)


def separate_var(csv_in,vars):
    df = pd.read_csv(csv_in)
    deli = r',|\||;|(\band\b)'
    for var in vars:
        sp_data = df[var].str.split(deli,expand=True)
        df = df.join(sp_data.add_prefix(f'{var}_'))
    return df



def parallelize_df(df,func,**kwargs):
    num_cores = multiprocessing.cpu_count()
    df_split = np.array_split(df,num_cores)
    mapfunc = partial(func,**kwargs)
    try:
        with Pool(num_cores) as pool:
            results = pool.map(mapfunc,df_split)
    except:
        logger.exception("Uncaught exception for parallelize_df")
        return None
    else:
        return pd.concat(results)


def add_fyear(report_period, year_end, annual=True):
    
    if isinstance(report_period,(date, datetime)) == False:
        try:
            report_period = datetime.strptime(report_period, "%Y-%m-%d")
        except:
            logger.exception("report period not recognized")
            return None

    calendar_year = report_period.year

    if annual == True:
        monthend = report_period.replace(day=calendar._monthlen(calendar_year,report_period.month)) 
        last_monthend = report_period.replace(day=1) - timedelta(days=1)

        if abs(report_period - monthend) <= abs(report_period - last_monthend):
            fyear_end = monthend
        else:
            fyear_end = last_monthend
    
    else:
        if isinstance(year_end, (float,int)):
            if math.isnan(year_end) == False:
                year_end = str(int(year_end)).zfill(4)
            else:
                logger.warning("no year end available, return calendar year")
                return calendar_year
        elif isinstance(year_end, str):
            year_end = year_end.zfill(4)
        
        cyear_end = ''.join([str(calendar_year),year_end])
        try:
            fyear_end = datetime.strptime(cyear_end,"%Y%m%d")
        except ValueError:
            cyear_end = cyear_end[:6] + '01'
            fyear_end = datetime.strptime(cyear_end,"%Y%m%d")
            fyear_end = fyear_end.replace(day=calendar._monthlen(calendar_year,fyear_end.month)) 

             
    new_day = calendar._monthlen(calendar_year-1,fyear_end.month)
    last_fyear_end = fyear_end.replace(year=calendar_year-1,day=new_day)

    period_past_fyearend = (report_period - fyear_end).days
    period_past_lastfyearend = (report_period - last_fyear_end).days

    if fyear_end.month <= 5:
        if period_past_fyearend > 15:
            return calendar_year
        else:
            return calendar_year - 1
    else:
        if period_past_fyearend > 15:
            return calendar_year + 1
        elif period_past_lastfyearend < 15:
            return calendar_year - 1
        else:
            return calendar_year

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6',
    'Host': 'www.sec.gov',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
}

def split_n(n,temp_folder,sort=False,sortkeys=None,intkeys=None):
    def split(func):
        @wraps(func)
        def wrapper(csv_in,folder,**kwargs):
            lists = mc.split_csv_num(csv_in, n, temp_folder)
            p_lists = [(split_csv,temp_folder) for split_csv in lists]
            mapfunc = partial(func,**kwargs)
            n_thrd = int(n/2)
            try:
                with ThreadPool(n_thrd) as pool:
                    results = pool.starmap(mapfunc, p_lists)
            except:
                return None
            csv_out_name = results[0].name[:-6]
            if sort == True:
                unsort_name = f'unsort_{csv_out_name}'
                unsort = mc.merge_csv(results,unsort_name,temp_folder)
                csv_out = folder / f'{csv_out_name}.csv'
                mc.sort_by_columns(unsort,csv_out,*sortkeys,intkeys=intkeys)
                return csv_out
            else:
                return mc.merge_csv(results,csv_out_name,folder)

        return wrapper
    return split
