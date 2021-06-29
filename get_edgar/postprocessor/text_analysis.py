import logging
import urllib.request
from bs4 import BeautifulSoup
# import nltk
import re
import time
import pandas as pd
import requests

from get_edgar.common.utils import headers

logger = logging.getLogger(__name__)

class text_page():
    __slots__ = 'link','parags','title','irr_parags'
    def __init__(self, link):
        self.link = link
        self.parags = self.__parags()

    def soup(self):
        if self.link:
            i = 0
            while True:
                try:
                    time.sleep(0.5)
                    htm = requests.get(self.link, headers=headers)
                    break   
                except (requests.exceptions.HTTPError):
                    logger.error(f'try wait a minute to reopen {self.link}')
                    time.sleep(70)
                except TimeoutError:
                    logger.error(f'try wait a minute to reopen {self.link}')
                    time.sleep(70)
                except Exception:
                    logger.error(f'try wait a minute to reopen {self.link}', exc_info=True)
                    i += 1
                    if i > 10:
                        logger.exception(f'wrong path for {self.link}')
                        return None
                    time.sleep(70)
            try:
                soup = BeautifulSoup(htm.text,'lxml')
            except Exception:   
                logger.exception(f'cannot make soup from {self.link}', exc_info=True)
                self.title = None
                return None
            else:
                self.title = text_page.get_title(soup) 
                return soup
        else:
            logger.warning('no link')
            return None

    @staticmethod
    def get_title(soup):
        if soup:
            try:
                return soup.title.get_text()
            except AttributeError:
                return None
            except Exception:
                logger.exception('Uncaught exception for get_title')
                return None
        else:
            return None

    def __parags(self):
        soup = self.soup()
        dparas = text_page.parags_d(soup,self.link)

        if dparas:
            all_real_para = [para for para in dparas if para.isreal]    
            if all_real_para:
                return all_real_para
            else:
                logger.warning(f'no real paragraph extracted from {self.link}')
                noreal_para = text_page.parags_noreal(dparas)
                self.irr_parags = bool(text_page.parags_exception(noreal_para))
                if self.irr_parags:
                    logger.warning(f'paragraphs cannot be categorized in {self.link}')
                return None
        else:
            return None

    @staticmethod
    def parags_d(soup,link):
        if soup:
            paras = soup.find_all(["p","div","td"])
            if paras:
                return [paragraph(para) for para in paras if (not para.find_all(["p","div","td"]))]
            else:
                logger.warning(f'no paragraph extracted from {link}')
                return None
        else:
            return None

    @staticmethod
    def parags_noreal(dparas):
        return [para for para in dparas if not para.isreal]

    @staticmethod
    def parags_exception(noreal_para):
        return [para.para for para in noreal_para if not (para.isimg() | para.isempty())]

    @staticmethod
    def parags_incompletes(all_parags):
        if all_parags is not None:
            return [para.isincomplete for para in all_parags]
        else:
            return None

    @staticmethod
    def parags_incompletesBeg(all_parags):
        if all_parags is not None:
            return [para.isincompleteBeg for para in all_parags]
        else:
            return None

    @staticmethod
    def parags_joined(all_parags):
        if all_parags is not None:
            incompleteEnd = text_page.parags_incompletes(all_parags)
            incompleteBeg = text_page.parags_incompletesBeg(all_parags)
            titles = [para.istitle for para in all_parags]
            intable = [para.intable for para in all_parags]
            len_all = len(all_parags)
            incompleteEnd[len_all-1] = 'END'

            iter_n = (num for num in reversed(range(len_all)))

            iter_m = (m for m in range(len_all))
            for m in iter_m:
                if incompleteEnd[m] == 'TBC':
                    for j in range(1,len_all-m):
                        if intable[m+j] is False:
                            break
                        else:
                            if titles[m+j] is False:
                                incompleteBeg[m+j] = True
                            next(iter_m,None)

            try:
                for num in iter_n:
                    if titles[num]:
                        continue
                    else:
                        for i in range(1,len_all-num):
                            if titles[num+i] is False:
                                if incompleteBeg[num+i] is False:
                                    if incompleteEnd[num] != 'TBC':
                                        break
                                    else:
                                        incompleteBeg[num+i] = True
                                all_parags[num].append(all_parags[num+i])
                                break
                        incompleteEnd[num] = 'END'
            except Exception:
                logger.exception('cannot generate joined paragraphs')

            joined = [all_parags[n] for n, v in enumerate(incompleteBeg) if v==False]
            return joined

        else:
            return None

    @staticmethod
    def parags_text(parags):          
        if parags is not None:
            return (para.text for para in parags)
        else:
            return None

    def section_slice(self,**kwargs):
        def cr_sbeg(para,**kwargs):
            return (paragraph.filtering(para.text,**kwargs) is not None) & (para.istitle)

        def cr_send(num,nbeg,para,**kwargs):
            return (num > nbeg) & (paragraph.filtering(para.text,**kwargs) == None) & (para.istitle)

        if self.parags:
            para_b = (num for num,para in enumerate(self.parags) if cr_sbeg(para,**kwargs))
            s_beg = next(para_b,None)

            if s_beg is not None:
                para_e = (num for num,para in enumerate(self.parags) if cr_send(num,s_beg,para,**kwargs))
                s_end = next(para_e,-1)
                return (s_beg,s_end)
            else:
                logger.debug(f'cannot find section begin for {self.link}')
                return None
        else:
            return None

    def section_text(self, **kwargs):
        s_slice = self.section_slice(**kwargs)
        if s_slice is not None:
            s_beg, s_end = s_slice
            return text_page.parags_text(self.parags[s_beg:s_end])
        else:
            return None        


    def filtered_parags(self,section_exc=None,joined=True,**kwargs):
        filtered = []
        if self.parags:
            if section_exc:
                s_beg, s_end = section_exc
                to_filter = self.parags[:s_beg] + self.parags[s_end:]
            else:
                to_filter = self.parags
            if joined:
                to_filter = text_page.parags_joined(to_filter)
            for para in to_filter:
                if paragraph.filtering(para.text,**kwargs) is not None:
                    filtered.append(para)
            if len(filtered) == 0:
                logger.debug(f'no filtered paragraphs in {self.link}')
                return None
            else:
                logger.info(f'found filtered paragraphs in {self.link}')
                return filtered
   
    def save_filtered_parags(self, save_file, append=False, **kwargs):
        filtered = self.filtered_parags(**kwargs)
        if filtered:
            if append:
                with open(save_file,'a',encoding='utf-8') as f:
                    f.write(f'{self.link}\n')
                    for para in filtered:
                        f.write(para.text)
                        f.write('\n\n')
            else:
                with open(save_file,'w',encoding='utf-8') as f:
                    f.write(f'\n{self.link}\n')
                    for para in filtered:
                        f.write(para.text)
                        f.write('\n\n')
            logger.info(f'filtered paragraphs saved for {self.link}')


class paragraph():
    __slots__ = 'para', 'istitle','isreal','isincomplete','isincompleteBeg','intable'
    def __init__(self, para):
        self.para = para
        self.intable = bool(para.find_parents("table"))
        self.istitle = self.__istitle() 
        self.isreal = self.__isreal()
        self.isincomplete = self.__isincomplete()
        self.isincompleteBeg = self.__isincomplete_beg()
    
    @property
    def text(self):
        return paragraph.clean_uni(self.para.get_text())

    def append(self, paragraph_b):
        self.para.append(' ')
        self.para.append(paragraph_b.text)
        self.para.append(' ')
        self.istitle = self.__istitle()
        # self.text = self.text + ' ' + paragraph_b.text

    def __istitle(self):
        text = self.text
        if text:
            bold_1 = re.compile(r"font-weight\s*:\s*bold",re.IGNORECASE)
            if self.para.has_attr('style'):
                cr_1 = bool(bold_1.search(self.para['style'])) # bold font
            else:
                cr_1 = False
            cr_2 = bool(text.isupper()) # all upper case
            if self.para.find('b'):
                cr_3 = bool(self.para.b.get_text() == self.para.get_text()) # bold font
            else:
                cr_3 = False
            if self.para.find('u'):
                cr_4 = bool(self.para.u.get_text() == self.para.get_text()) # all underscore
            else:
                cr_4 = False
            notcapitalized = re.compile(r"\b[a-z]{4,}\b")
            if notcapitalized.search(text):
                cr_5 = False
            else:
                if self.intable:
                    cr_5 = False
                else:
                    cr_5 = True
            
            return cr_1 | cr_2 | cr_3 | cr_4 | cr_5
        else:
            return False

    def __ispagenum(self):
        text = self.text
        if text.replace("-"," ").strip().isdigit():
            return True
        else:
            words = re.findall(r'[a-z]+',text.lower())
            pagewords = {'page','of','i','ii','iii','iv','v','vi','vii','viii','xi','x'}
            if words: 
                if set(words).issubset(pagewords):
                    return True
            letter_p = re.compile(r'^[A-Z]{1,2}\s*-\s*\d{1,3}\s*\Z')
            if letter_p.search(text.strip()):
                return True
        return False


    def __isreal(self):
        real_para = re.compile(r'[a-zA-Z]+\b\W+\w+')
        only_symbol = re.compile(r'^\W+\Z')
        text = self.text
        if text:
            cr_pos = bool(real_para.search(text)) | self.istitle | self.intable
            cr_neg_1 = bool(re.sub(r'[^a-zA-Z]','',text).lower() == 'tableofcontents')
            cr_neg_2 = self.__ispagenum()
            cr_neg_3 = bool(only_symbol.search(text))
            cr_neg_4 = bool(re.sub(r'\W','',text).isdigit())
            return cr_pos & (not cr_neg_1) & (not cr_neg_2) & (not cr_neg_3) & (not cr_neg_4)
        else:
            return False

    def __isincomplete(self):
        # intable = bool(self.para.find_parents("table"))
        incomplete_p1 = re.compile(r'(:|;|,)\W*\Z')
        incomplete_p2 = re.compile(r'((and)|(or))\s*\Z')
        incomplete_1 = bool(incomplete_p1.search(self.text))
        incomplete_2 = bool(incomplete_p2.search(self.text))
        complete_p1 = re.compile(r'\.\W*\Z')
        complete_1 = bool(complete_p1.search(self.text))

        if self.istitle is False:
            if incomplete_1 | incomplete_2:
                return 'TBC'
            elif complete_1:
                return 'END'
            else:
                # if intable:
                #     return 'END'
                # else:
                return 'TBD'
        else:
            return 'END'
        

    def __isincomplete_beg(self):
        if self.istitle is False:
            incomplete_p1 = re.compile(r'^[a-z]+\b')
            incomplete_1 = bool(incomplete_p1.search(self.text))

            incomplete_p2 = re.compile(r'^\(?\w\)\s+\w+\b')
            incomplete_2 = bool(incomplete_p2.search(self.text))

            incomplete_p3 = re.compile(r'^[^\(]+\)')
            incomplete_3 = bool(incomplete_p3.search(self.text))


            return incomplete_1 | incomplete_2 | incomplete_3
        else:
            return False

    def isimg(self):
        if not self.isreal:
            return bool(self.para.find("img"))
        else:
            return False

    def isempty(self):
        if not self.isreal:
            try:
                self.para.font.decompose()
            except AttributeError:
                pass
            try:
                self.para.br.decompose()
            except AttributeError:
                pass
            try:
                self.para.a.decompose()
            except AttributeError:
                pass
            return not bool(self.para.contents) 

    @staticmethod
    def filtering(text, filters, excludes=None, p_ex=None, p_1=None, p_2=None):
        if text:
            if excludes is not None:
                for exclude in excludes:
                    if paragraph.keyword_include(text,exclude,pattern=p_ex):
                        return None
            fits = []
            for filter_info in filters:
                if isinstance(filter_info[0],str):
                    fit_1 = paragraph.keyword_include(text,filter_info[0],pattern=p_1,flag=filter_info[1])
                    if fit_1:
                        fits.append(fit_1)
                elif isinstance(filter_info[0],tuple):
                    for word_1 in filter_info[0][0]:
                        fit_1 = paragraph.keyword_include(text,word_1,pattern=p_1,flag=filter_info[0][1])
                        if fit_1:
                            for word_2 in filter_info[1][0]:
                                fit_2 = paragraph.keyword_include(text,word_2,pattern=p_2,flag=filter_info[1][1])
                                if fit_2:
                                    fits += [fit_1,fit_2]
            if fits:
                return tuple(sorted(set(fits)))
        return None

    
    @staticmethod
    def predict_sents(series, model):
        if series is not None:
            series = series.dropna()
            if series.empty == False:
                pred = model.predict(series)
                frame = {'sentence': series, 'pred': pred}
                df = pd.DataFrame(frame)
                df = df.drop_duplicates()
                pos_sent = df.loc[df['pred']==1]['sentence']
                if pos_sent.empty == False:
                    return pos_sent.size, " \n ".join(pos_sent.tolist())
        return 0, None


    @staticmethod
    def keyword_include(text,keyword,pattern=None,flag='i'):
        if pattern is None:
            if flag == 'i':
                if keyword in text.lower():
                    return keyword.strip()
            else:
                if keyword in text:
                    return keyword.strip()
            return None

        elif isinstance(pattern,int):
            phrase_words = keyword.split()
            text_words = text.split()
            search_range = len(phrase_words) + pattern
            searched = set()
            for i in range(len(text_words)-search_range):
                if paragraph.keyword_include(text_words[i],phrase_words[0],flag=flag):
                    for j in range(1,search_range):
                        if paragraph.keyword_include(text_words[i+j],phrase_words[1],flag=flag):
                            searched.add(' '.join(text_words[i:i+j+1]))
                            if searched:
                                return list(searched)[0]
            return None

    
    def para_ispagebreak(self):
        page_num = bool(self.para.find(style=re.compile('pagenumber')))
        page_barea = bool(self.para.find(style=re.compile('pagebreak')))
        page_bafter = bool(self.para.find(style=re.compile('page-break-after:always')))

        return page_num | page_barea | page_bafter

    @staticmethod
    def clean_uni(text):
        unicode_list = ['\xc2','\xa0','\xe2','\x98','\x90','\xa7','\n', \
            '\xe2\x98\x90','\x95','\x97','\x9f']
        for co in unicode_list:
            text = text.replace(co," ")
        text = text.replace('\x92',"'")
        text = text.replace('\x93','"')
        text = text.replace('\x94','"')
        text = text.replace('\x96',' - ')
        text = text.replace("\'","'")
        text = ' '.join(text.split())
        return text.strip()
