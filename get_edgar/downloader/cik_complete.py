import logging
import urllib.request
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

from get_edgar.downloader.indexdownloader import index_url

class EDGAR_Index():
    def __init__(self,year):
        self.year = year
        self.urls = index_url(year)


    def extract_table(self,i):
        try:
            master = urllib.request.urlopen(self.urls[i])          
        except urllib.error.HTTPError:
            return None
        to_skip = list(range(9)) + [10]
        return pd.read_table(master,sep='|',encoding='cp1252',skiprows=to_skip,parse_dates=['Date Filed'],\
            infer_datetime_format=True)


    def concat_table(self):
        index_all = [self.extract_table(i) for i in range(4)]
        index_v = [index for index in index_all if index is not None]
        if index_v:
            return pd.concat(index_v)
        else:
            return None
        

    def cik_cname_id(self):
        df_all = self.concat_table()
        if df_all is not None: 
            df_all = df_all.sort_values(['CIK', 'Date Filed'])
            df_all['company_p'] = df_all.groupby('CIK')['Company Name'].shift(periods=1)
            df_all['company_p'] = df_all['company_p'].fillna('new')
            df_all['new_company'] = df_all.apply(lambda row: (row['Company Name'] != row['company_p']) | (row['company_p'] == 'new'), axis=1)
            df_all['companyid'] = (df_all['new_company'] == True).cumsum()
            return df_all
        return None


    def extract_period(self):
        df_all = self.cik_cname_id()
        if df_all is not None:
            df_id = df_all.groupby(['companyid','CIK','Company Name']).agg({'Date Filed':[np.min, np.max],'Filename': np.size})
            return df_id.droplevel('companyid')
        return None

