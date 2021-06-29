import json
import re
import multiprocessing
import sys
import logging
import logging.handlers
import logging.config
import os
import argparse
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
from functools import partial
from functools import wraps

sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))

import get_edgar.common.my_csv as mc
import get_edgar.downloader.indexdownloader as ind_dl
import get_edgar.downloader.filedownloader as f_dl
import get_edgar.extractor.fileinfo_extractor as finfo_ext 
import get_edgar.extractor.textinfo_extractor as tinfo_ext
from get_edgar.postprocessor import text_analysis as text_ana
import get_edgar.options as opts
import get_edgar.common.utils as utils
from get_edgar.common.utils import split_n


def main():
    args = opts.parseargs()

    # set logger
    docs = Path(__file__).resolve().parent.parent/ 'docs'

    logger_conf = docs / 'logging.conf'
    logger = logging.getLogger('get_edgar.main')
    logging.config.fileConfig(logger_conf)

    logger.info(f'{vars(args)}')

    # set folder for files
    topfolder = opts.read_topfolder(args)

    # set file folders according to the tag
    file_folder = topfolder / args.tag
    if file_folder.exists() == False:
        file_folder.mkdir()

    # set index/finfo/tinfo/text folders
    index_folder = file_folder / 'index'
    finfo_folder = file_folder / 'finfo'
    tinfo_folder = file_folder / 'tinfo'
    text_folder = file_folder / 'text'
    temp_folder = file_folder / 'temp'
    folders = {
        'index_folder': index_folder,
        'finfo_folder': finfo_folder,
        'tinfo_folder': tinfo_folder,
        'text_folder' : text_folder,
        'temp_folder' : temp_folder
    }
    for folder in folders.values():
        if folder.exists() == False:
            folder.mkdir()

    # shorten arguments
    tag = args.tag
    start_year = args.start_year
    end_year = args.end_year

    # set form types and ciks
    form_types = tuple(args.form_types)
    form_types_ex8k = tuple(form_type for form_type in form_types if form_type != '8-K')
    
    ciks = opts.read_cik(args)

    # set event file, if applicable
    evtcsv = opts.read_evt(args,ciks)

    # change item file to sets, if applicable
    items = opts.read_items(args)

    # change filter file to sets, if applicable
    filters = opts.read_filts(args.filterfile)

    # change excludes file to set, if applicable
    excludes = opts.read_filts(args.excludesfile)


    # define split functions
    if args.split != 0:
        splitn = args.split
        cfkeys = ('cik','filing_date')

        @split_n(splitn,temp_folder,sort=True,sortkeys=cfkeys,intkeys=('cik'))
        def main_fileinfo(csv_in,folder):
            return finfo_ext.save_file_info(csv_in,folder)

        @split_n(splitn,temp_folder,sort=True,sortkeys=cfkeys,intkeys=('cik'))
        def main_textinfo(csv_in,folder,**kwargs):
            return tinfo_ext.save_textanalysis(csv_in,folder,**kwargs)
    else:
        splitn = 0
        
        def main_fileinfo(csv_in,folder):
            return finfo_ext.save_file_info(csv_in,folder)

        def main_textinfo(csv_in,folder,**kwargs):
            return tinfo_ext.save_textanalysis(csv_in,folder,**kwargs)
       

    # set input csvs for each purpose   
    if args.purpose == 'index_evtfilter':
        index_csvs = [
            index_folder / f'index_{tag}_{form_type}_{year}.csv' \
            for form_type in form_types for year in list(range(start_year, end_year+1))
        ]

        for index_csv in index_csvs:
            if index_csv.exists() == False:
                print(f'Need to download {index_csv.name} first')
                return
    
    if args.purpose == 'finfo_evtfilter':
        index_csvs = [
            finfo_folder / f'info_{tag}_{form_type}_{year}.csv' \
            for form_type in form_types for year in list(range(start_year, end_year+1))
        ]

        for index_csv in index_csvs:
            if index_csv.exists() == False:
                print(f'Need to download {index_csv.name} first')
                return
    
    if args.purpose == 'file_info':
        if evtcsv:
            index_csvs = [
                index_folder / f'index_{tag}_{form_type}_{year}_{args.mperiods}m.csv' \
                for form_type in form_types for year in list(range(start_year, end_year+1))
            ]
        else:
            index_csvs = [
                index_folder / f'index_{tag}_{form_type}_{year}.csv' \
                for form_type in form_types for year in list(range(start_year, end_year+1))
            ]

        for index_csv in index_csvs:
            if index_csv.exists() == False:
                print('Need to download index first')
                return
    
    elif args.purpose == 'extract_items':
        if evtcsv:
            finfo8k_csvs = [
                finfo_folder / f'info_{tag}_8-K_{year}_{args.mperiods}m.csv' \
                for year in list(range(start_year, end_year+1))
            ]
        else:
            finfo8k_csvs = [
                finfo_folder / f'info_{tag}_8-K_{year}.csv' \
                for year in list(range(start_year, end_year+1))
            ]

        
        for info_csv in finfo8k_csvs:
            if info_csv.exists() == False:
                print('Need to add filing information first')
                return
        
        if items is not None and len(items):
            pass
        else:
            print('Need to input items to extract items')
            return

    elif args.purpose == 'text_info':
        if evtcsv:
            finfoex8k_csvs = [
                finfo_folder / f'info_{tag}_{form_type}_{year}_{args.mperiods}m.csv' \
                for form_type in form_types_ex8k for year in list(range(start_year, end_year+1))
            ]
            finfo_csvs = [
                finfo_folder / f'info_{tag}_{form_type}_{year}_{args.mperiods}m.csv' \
                for form_type in form_types for year in list(range(start_year, end_year+1))
            ]
            items_csvs = [
                finfo_folder / f'item_{tag}_8-K_{year}_{args.mperiods}m.csv' \
                    for year in list(range(start_year, end_year+1))
            ] 
        else:
            finfoex8k_csvs = [
                finfo_folder / f'info_{tag}_{form_type}_{year}.csv' \
                for form_type in form_types_ex8k for year in list(range(start_year, end_year+1))
            ]
            finfo_csvs = [
                finfo_folder / f'info_{tag}_{form_type}_{year}.csv' \
                for form_type in form_types for year in list(range(start_year, end_year+1))
            ]
            items_csvs = [
                finfo_folder / f'item_{tag}_8-K_{year}.csv' \
                    for year in list(range(start_year, end_year+1))
            ] 

        for info_csv in finfo_csvs:
            if info_csv.exists() == False:
                print('Need to add filing information first')
                return

        if '8-K' in form_types and items is not None and len(items):
            for info_csv in items_csvs:
                if info_csv.exists() == False:
                    print('Need to extract items for 8-K first')
                    return       

    # start get_edgar
    if args.purpose == 'dl_index':
        try:
            ind_dl.dl_index(index_folder,start_year,end_year,form_types,tag,ciks=ciks)
            logger.info(f'index for {tag} downloaded')
        except Exception:
            logger.exception(f'failed to download index {tag}')
    
    if args.purpose in ('index_evtfilter', 'finfo_evtfilter'):
        kwargs = {'evt_csv':evtcsv, 'evtdate':'disclosure_date', 'mperiods':args.mperiods}
        mapfunc = partial(ind_dl.evt_filter,**kwargs)
        n_subs = end_year - start_year + 1
        try:
            with ThreadPool(n_subs) as pool:
                pool.map(mapfunc,index_csvs)
        except:
            logger.exception('failed to filter files')

    if args.purpose == 'file_info':
        for index_csv in index_csvs:
            try: 
                main_fileinfo(index_csv,finfo_folder)
            except:
                logger.exception(f'failed to create file info for {index_csv.name}') 
            else:
                logger.info(f'file info for {index_csv.name} created')

    elif args.purpose == 'extract_items':
        item_list = [(finfo_csv,items) for finfo_csv in finfo8k_csvs]
        n_subs = end_year - start_year + 1
        try:
            with ThreadPool(n_subs) as pool:
                pool.starmap(finfo_ext.select_items, item_list)
            logger.info('items extracted')  
        except Exception:
            logger.exception('failed to extract items')


    elif args.purpose == 'text_info':
        if tag == 'AC':
            kwargs = {'save_txt':args.save_txt, 'suffix':tag, 'filters':filters, 'p_1':4, 'excludes':excludes}
        elif tag == 'breach':
            kwargs = {'save_txt':args.save_txt, 'suffix':tag, 'filters':filters, 'excludes':excludes}
        # mapfunc = partial(main_textinfo,**kwargs)
        if '8-K' in form_types:
            if items is not None and len(items):
                all_csvs = items_csvs + finfoex8k_csvs
            else:
                all_csvs = finfo_csvs
        else:
            all_csvs = finfo_csvs

        for info_csv in all_csvs:
            try:
                main_textinfo(info_csv,tinfo_folder,**kwargs)
            except Exception:
                logger.exception(f'failed to create text info for {info_csv.name}')
            else:
                logger.info(f'text info created for {info_csv.name}')


if __name__ == "__main__":
    main()
        
