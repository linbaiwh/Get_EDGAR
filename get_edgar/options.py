import argparse
from pathlib import Path
import logging
import logging.config
import sys
import pandas as pd

logger = logging.getLogger(__name__)


def parseargs():
    parser = argparse.ArgumentParser(prog='Get EDGAR')
    parser.add_argument('purpose')
    parser.add_argument('tag')
    parser.add_argument('start_year',type=int)
    parser.add_argument('end_year',type=int)
    parser.add_argument('form_types',nargs='+')

    parser.add_argument('--topfolder',type=Path,nargs='?',const=None,default=None)
    parser.add_argument('--cikfile',type=Path,nargs='?',const=None,default=None)
    parser.add_argument('--cikset',nargs='?',const=None,default=None)
    parser.add_argument('--evtfilter',type=bool,nargs='?',const=False,default=False)
    parser.add_argument('--mperiods',type=int,nargs='?',const=12,default=12)
    parser.add_argument('--itemfile',type=Path,nargs='?',const=None,default=None)
    parser.add_argument('--itemset',nargs='*',default=None)
    parser.add_argument('--filterfile',type=Path,nargs='?',const=None,default=None)
    parser.add_argument('--excludesfile',type=Path,nargs='?',const=None,default=None)
    # parser.add_argument('--filterset',nargs='*',const=None,default=None)
    parser.add_argument('--save_txt',type=bool, nargs='?',const=False,default=False)
    parser.add_argument('--split',type=int,nargs='?',const=0,default=0)

    return parser.parse_args()

def read_args(args):
    global topfolder, ciks, evtcsv, items, filters, excludes
    topfolder = read_topfolder(args)
    ciks = read_cik(args)
    evtcsv = read_evt(args,ciks)
    items = read_items(args)
    filters = read_filts(args.filterfile)
    excludes = read_filts(args.excludesfile)

def read_topfolder(args):
    if args.topfolder is not None:
        topfolder = args.topfolder.resolve()
        if topfolder.is_absolute == False:
            sys.exit('topfolder path need to be absolute')
    else:
        topfolder = Path(__file__).resolve().parents[1] / 'data'
    
    try:
        topfolder.mkdir(parents=True,exist_ok=True)
    except FileNotFoundError:
        sys.exit('topfolder path not valid')
    else:
        return topfolder

def read_cik(args):
    if args.cikset is not None:
        ciks = tuple(args.cikset.split())
        if len(ciks) == 0:
            logger.warning('no ciks specified in cikset')
            return None
        else:
            return ciks
    elif args.cikfile is not None:
        if args.cikfile.is_file() and args.cikfile.suffix == '.csv':
            return args.cikfile.resolve()
        else:
            sys.exit('cik file path not valid')
    else:
        return None

def read_evt(args,ciks):
    if args.evtfilter == True:
        if isinstance(ciks,Path):
            if ciks.is_file():
                if args.mperiods != 0:
                    return ciks
                else:
                    sys.exit('mperiods(number of periods after events) not exists')
            else:
                sys.exit('event date file path not valid')
        else:
            sys.exit('event date file not exists')
    else:
        return None

def read_items(args):
    if args.itemset is not None:
        if len(args.itemset):
            return tuple(args.itemset)
        else:
            items = None
    if args.itemfile is not None:
        try:
            with open(args.itemfile,'r') as fitem:
                items = tuple(fitem.read().splitlines())
                if len(items) == 0:
                    logger.warning('no items specified in itemfile')
                    return None
                else:
                    return items
        except OSError:
            sys.exit('item file path not valid')

def read_filts(filepath):
    if filepath is not None:
        try:
            if filepath.suffix == ".txt":
                with open(filepath,'r') as fitem:
                    flines = tuple(fitem.read().splitlines())
            elif filepath.suffix == ".xlsx":
                filts_df = pd.read_excel(filepath,header=None,names=['word_1','flag_1','word_2','flag_2'])
                filts_df['word_1'] = filts_df['word_1'].str.split(',')
                filts_df['word_2'] = filts_df['word_2'].str.split(',')
                filts_df['one_word'] = filts_df['word_2'].isna()

                filts_df['filts'] = filts_df.apply(lambda row: ((row['word_1'],row['flag_1']),(row['word_2'],row['flag_2'])) if row['one_word']==False else (row['word_1'][0],row['flag_1']), axis=1)
                flines = filts_df['filts'].tolist()
                
            if len(flines) == 0:
                logger.warning(f'no filters specified in {filepath.name}')
                return None
            else:
                return flines

        except OSError:
            sys.exit(f'{filepath} not valid')
    else:
        return None