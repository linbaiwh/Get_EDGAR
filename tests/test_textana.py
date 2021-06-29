import sys
import os
from pathlib import Path
import pytest

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_edgar.postprocessor.text_analysis import text_page, paragraph
from get_edgar import options as opts


docs = Path(__file__).resolve().parent.parent/ 'docs'
testsfolder = Path(__file__).resolve().parents[1] / 'data' / 'tests'
breach_sents_train = testsfolder / 'breach_sents_test.xlsx'

datafolder = Path(__file__).parents[1] / 'data'
file_eg = datafolder / 'get_text.txt'

@pytest.fixture
def page_img():
    return text_page("https://www.sec.gov/Archives/edgar/data/1297587/000129758717000112/exhibit991.htm")

@pytest.fixture
def dparas_page_img(page_img):
    return page_img.parags_d(page_img.soup(),page_img.link)


@pytest.fixture
def page_unicode():
    return text_page("https://www.sec.gov/Archives/edgar/data/718332/000114036119006607/form8k.htm")

def test_clean_uni(page_unicode):
    joined_parags = text_page.parags_joined(page_unicode.parags)
    for parag in joined_parags:
        print(parag.text)

def test_isimg(dparas_page_img):
    assert text_page.parags_noreal(dparas_page_img)[11].isimg() == True

def test_isempty(dparas_page_img):
    assert text_page.parags_noreal(dparas_page_img)[10].isempty() == True

def test_parags_exception(dparas_page_img):
    noreal = text_page.parags_noreal(dparas_page_img)
    assert text_page.parags_exception(noreal) == False

def test_has_irr(page_img):
    assert page_img.irr_parags == False

@pytest.fixture
def page_pagenum():
    # return text_page("https://www.sec.gov/Archives/edgar/data/1368458/000119312518035397/d550388dex991.htm")
    # return text_page("https://www.sec.gov/Archives/edgar/data/1348036/000095012318009405/filename1.htm")
    return text_page("https://www.sec.gov/Archives/edgar/data/927971/000121465918004816/r627180s1.htm")


@pytest.fixture
def page_has_filter():
    return text_page("https://www.sec.gov/Archives/edgar/data/13239/000001323919000012/hexion8k.htm")


@pytest.fixture
def page_no_filter():
    return text_page("https://www.sec.gov/Archives/edgar/data/4977/000000497719000009/afl8kearningsrelease-q42018.htm")

def test_isreal(page_pagenum):
    parags = page_pagenum.parags
    joined_parags = text_page.parags_joined(parags)
    assert len(joined_parags) < len(parags)

def test_join(page_pagenum):
    parags = page_pagenum.parags
    joined_parags = text_page.parags_joined(parags)
    with open(file_eg, 'w', encoding="utf-8") as f:
        for num,para in enumerate(joined_parags):
            f.write(f'Paragraph {num}')
            f.write('\n')
            f.write(para.text)
            f.write('\n\n')
    assert len(joined_parags) < len(parags)


def test_predit_model(page_pagenum, Incident_model):
    joined_parags = text_page.parags_joined(page_pagenum.parags)
    fitted_parags = [parag.model_pred_classify(Incident_model) for parag in joined_parags]
    assert fitted_parags == []

def test_section_slice(page_pagenum):
    section_fls = page_pagenum.section_slice(filters=('FORWARD-LOOKING','SAFE HARBOR'))
    assert section_fls is not None

filter_file = datafolder / 'breach_dict.xlsx'
filters = opts.read_filts(filter_file)

def test_filtering_nopattern():
    text = "It is important to note that no sensitive personal information, such as social security number or personally identifying information, was affected in this incident."
    result = paragraph.filtering(text,filters)
    assert result == ('affect','incident','information','security','this incident')


def test_filtered_para(page_has_filter):
    filtered = page_has_filter.filtered_parags(filters=filters)
    for para in filtered:
        print(para.text)
    assert len(filtered) > 0

def test_nofiltered_para(page_no_filter):
    filtered = page_no_filter.filtered_parags(filters=filters)
    assert filtered is None


def test_keyword_include_nopattern():
    text = "It is important to note that no sensitive personal information, such as social security number or personally identifying information, was affected in this incident."
    keyword = " this incident"
    include = paragraph.keyword_include(text,keyword)
    assert include == "this incident"

def test_keyword_include_wpattern():
    text = "Audit, Compliance, and Risk Committee oversees the following aspects:"
    keyword = "audit committee"
    include = paragraph.keyword_include(text, keyword, pattern=4)
    assert include == "Audit, Compliance, and Risk Committee"


def test_filtering_wpattern():
    text_1 = "Audit, Compliance, and Risk Committee oversees the following aspects: financial reporting, regulation compliance for Cyberbonics."
    text_2 = "Audit, Compliance, and Risk Committee oversees the following aspects: financial reporting, cybersecurity and regulation compliance."
    searching = [((["audit committee"],'i'),([" cyber", " data security"],'s'))]
    result_1 = paragraph.filtering(text_1, searching, p_1=4)
    result_2 = paragraph.filtering(text_2, searching, p_1=4)
    assert result_1 == None
    assert result_2 == ("Audit, Compliance, and Risk Committee", "cyber")
