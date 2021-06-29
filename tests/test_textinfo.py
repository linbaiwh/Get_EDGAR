import sys
import os
from pathlib import Path

import pytest
import pandas as pd


sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import get_edgar.extractor.textinfo_extractor as tinfo_ext
import get_edgar.options as opts
import get_edgar.common.my_csv as mc

datafolder = Path(__file__).parent.parent / 'data'

# test_20f = datafolder / 'tests' / 'tinfo_20-F.csv'

breach_temp_folder = Path(r'F:\SEC filing\breach\temp')
ac_temp_folder = Path(r'F:\SEC filing\AC\temp')


def prepare_filter_args(tag):
    filterfile = datafolder / f'{tag}_dict.xlsx'
    excludefile = datafolder / f'breach_ex.txt'
    filters = opts.read_filts(filterfile)
    excludes = opts.read_filts(excludefile)
    if tag == 'AC':
        return {'suffix':tag, 'filters':filters, 'p_1': 4, 'excludes':excludes}
    elif tag == 'breach':
        return {'suffix':tag, 'filters':filters, 'excludes':excludes}


temp_8K = breach_temp_folder / 'info_breach_8-K_2019_6m_6.csv'
temp_8K_breach = breach_temp_folder / 'breach_8-K_2019_6m_6.csv'

temp_def = ac_temp_folder / 'info_AC_DEF14A_2008_5.csv'
temp_def_ac = ac_temp_folder / 'AC_DEF14A_2008_5.csv'

def info_row_eg(csv_in):
    rows = tinfo_ext.get_info_row(csv_in)
    return rows[1:11]

@pytest.mark.parametrize("tag",['breach', 'AC'])
def test_add_analysis(tag):
    args = prepare_filter_args(tag)
    if tag == 'breach':
        rows_eg = info_row_eg(temp_8K)
        temp_saved = temp_8K_breach
    elif tag == 'AC':
        rows_eg = info_row_eg(temp_def)
        temp_saved = temp_def_ac

    all_info = [tinfo_ext.add_textanalysis(r,**args) for r in rows_eg]
    info_rows = [row for (row,itext,wfn) in all_info]
    mc.save_dict_csv(info_rows,temp_saved)

@pytest.mark.parametrize("tag",['breach', 'AC'])
def test_save_analysis(tag):
    kwargs = prepare_filter_args(tag)
    kwargs.update({'save_txt':False,})
    if tag == 'breach':
        csv_in = temp_8K
        temp_folder = breach_temp_folder
    elif tag == 'AC':
        csv_in = temp_def
        temp_folder = ac_temp_folder

    csv_out = tinfo_ext.save_textanalysis(csv_in,temp_folder,**kwargs)
    df = mc.read_file_df(csv_out)
    df_in = mc.read_file_df(csv_in)
    assert df.shape[0] == df_in.shape[0]


# from get_edgar.common import my_csv as mc
# import get_edgar.extractor.fileinfo_extractor as finfo_ext
# import get_edgar.postprocessor.text_analysis as text_ana

# logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# # index_folder = Path(r'F:\SEC filing\index')
# # test_index = index_folder / 'test_index.csv'

# finfo_folder = Path(r'C:\Users\linbai\Dropbox\Research\code\Get_EDGAR\data\finfo')

# # test_8k = index_folder / 'sec_dis_8-K_2018.csv'

# # info_8k = finfo_ext.save_file_info(test_8k,index_folder)

# # info_evt = index_folder / 'otherevents_2018.csv'
# info_evt = finfo_folder / 'info_test_8-K_2018.csv'
# sorted_evt = finfo_folder / 'sorted_test.csv'

# # mc.sort_by_columns(info_evt,sorted_evt,'cik','filing_date')

# # items = {'Other Events'}

# # finfo_ext.select_items(info_evt,items)

# # mc.text_filter(info_8k,info_evt,'items',otherevents)

# # test_row = tinfo_ext.get_info_row(info_evt)[5]
# # print(test_row)
# # links = tinfo_ext.get_filepage(test_row)
# # page = links[0][1]
# # print(page)
# # print(type(links[0]))

# # page_eg = text_ana.text_page(page)

# # test list comprehension & if condition
# all_info = [
#     ('yes',3),
#     ('what',2),
#     ('no',0),
#     ('xyz',0)
# ]
# text = [itext for (itext, wfn) in all_info if wfn]
# print(text)



# # out_8k = index_folder / 'breach_2018.csv'

# filters = {'security incident', 'data security', 'data breach','information security',\
#             'cyber','attack','security breach','hack'}

# # has_breach = page_eg.has_filtered_parags(filters)
# # print(has_breach)

# # file_eg = index_folder / "example_file.txt"

# # row = {'cik':234253, 'form_type':'8-K'}

# # with open(file_eg,'a',encoding='utf-8') as fs:
# #     fs.write('---------- Filings Start ----------\n')
# #     fs.write(f"cik : {row['cik']}")
# #         company name : {test_row.get('conm')}\n \
# #         form type : {test_row.get('form_type')}\n \
# #         filing date : {test_row.get('filing_date')}\n\n')

# # nr = tinfo_ext.add_textanalysis(test_row,filters,'breach',save_para=file_eg)
# # print(nr)

# # tinfo_ext.save_textanalysis(info_evt,out_8k,filters=filters,suffix='breach',save_para=file_eg)
# # tinfo_ext.save_textanalysis(info_evt,finfo_folder,filters=filters,suffix='breach')

# logging.info('task completed')
