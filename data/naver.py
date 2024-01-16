from data import Info, Query, Variable, Schema, Field
from data import Rename

import re


###################################################################
########################### Naver Stock ###########################
###################################################################

def _drop_colon(__s: str) -> str:
    return __s.split(" : ")[-1] if isinstance(__s, str) and (" : " in __s) else __s

def _from_percentage(__n: float) -> float:
    try: return float(re.sub(r"[^\d.-]",'',str(__n))) / 100
    except: return None


NAVER_STOCK_INFO_QUERY = lambda: Query(
    Variable(name="code", type="STRING", desc="종목코드", iterable=True),
    Variable(name="stockType", type="STRING", desc="종목종류", iterable=True),
)


NAVER_COMPANY_INFO_SCHEMA = lambda: Schema(
    Field(name="code", type="STRING", desc="종목코드", mode="QUERY", path=["code"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="QUERY", path=["stockType"]),
    Field(name="name", type="STRING", desc="종목명", mode="NULLABLE", path=["name"]),
    Field(name="sector", type="STRING", desc="섹터", mode="NULLABLE", path=["sector"], apply=_drop_colon),
    Field(name="industry", type="STRING", desc="산업", mode="NULLABLE", path=["industry"], apply=_drop_colon),
    Field(name="bps", type="INTEGER", desc="BPS", mode="NULLABLE", path=["bps"], strict=False),
    Field(name="per", type="FLOAT", desc="PER", mode="NULLABLE", path=["per"], strict=False),
    Field(name="sectorPer", type="FLOAT", desc="업종PER", mode="NULLABLE", path=["sectorPer"], strict=False),
    Field(name="pbr", type="FLOAT", desc="PER", mode="NULLABLE", path=["pbr"], strict=False),
    Field(name="dividentYield", type="FLOAT", desc="PER", mode="NULLABLE", path=["dividentYield"], apply=_from_percentage),
)

NAVER_ETF_INFO_SCHEMA = lambda: Schema(
    Field(name="code", type="STRING", desc="종목코드", mode="QUERY", path=["code"]),
    Field(name="stockType", type="STRING", desc="종목종류", mode="QUERY", path=["stockType"]),
    Field(name="name", type="STRING", desc="종목명", mode="NULLABLE", path=["CMP_KOR"]),
    Field(name="url", type="STRING", desc="주소", mode="NULLABLE", path=["URL"]),
    Field(name="en_name", type="STRING", desc="영문명", mode="NULLABLE", path=["CMP_ENG"]),
    Field(name="index", type="STRING", desc="인덱스", mode="NULLABLE", path=["BASE_IDX_NM_KOR"]),
    Field(name="manager", type="STRING", desc="운용사", mode="NULLABLE", path=["ISSUE_NM_KOR"]),
    Field(name="etfType", type="STRING", desc="ETF종류", mode="NULLABLE", path=["ETF_TYP_SVC_NM"]),
    Field(name="totalPay", type="FLOAT", desc="", mode="NULLABLE", path=["TOT_PAY"]),
)


NAVER_STOCK_INFO_INFO = lambda: Info(
    query = NAVER_STOCK_INFO_QUERY(),
    company = NAVER_COMPANY_INFO_SCHEMA(),
    etf = NAVER_ETF_INFO_SCHEMA(),
)


###################################################################
########################## Naver Investor #########################
###################################################################

INDEX_CATEGORY = {"KOSPI":"01", "KOSDAQ":"02", "FUTURES":"03"}
INDEX_DESCRIPTION = {"KOSPI":"코스피", "KOSDAQ":"코스닥", "FUTURES":"선물"}

INSTITUTION_COLUMNS = ["financial","insurance","mutualFund","bank","otherInstitutions","pensionFund"]
INVESTOR_COLUMNS = ["individual","foreign","institutional"] + INSTITUTION_COLUMNS + ["others"]

INSTITUTION_NAMES = ["금융투자", "보험", "투신(사모)", "은행", "기타금융기관", "연기금등"]
INVESTOR_NAMES = ["개인", "외국인", "기관계"]+INSTITUTION_NAMES+["기타법인"]


NAVER_INVESTOR_QUERY = lambda: Query(
    Variable(name="startDate", type="DATE", desc="시작일자", iterable=False, default=None),
    Variable(name="endDate", type="DATE", desc="종료일자", iterable=False, default=None),
    Variable(name="indexType", type="STRING", desc="인덱스", iterable=False, default="KOSPI"),
    Variable(name="size", type="INTEGER", desc="개수", iterable=False, default=None),
    Variable(name="pageStart", type="INTEGER", desc="페이지시작", iterable=False, default=1),
)


NAVER_INVESTOR_SCHEMA = lambda: Schema(
    Field(name="date", type="DATE", desc="날짜", mode="NULLABLE", path=["date"]),
    Field(name="indexType", type="STRING", desc="인덱스", mode="QUERY", path=["indexType"], apply=Rename(INDEX_DESCRIPTION)),
    Field(name="individual", type="INTEGER", desc="개인", mode="NULLABLE", path=["individual"]),
    Field(name="foreign", type="INTEGER", desc="외국인", mode="NULLABLE", path=["foreign"]),
    Field(name="institutional", type="INTEGER", desc="기관계", mode="NULLABLE", path=["institutional"]),
    Field(name="financial", type="INTEGER", desc="금융투자", mode="NULLABLE", path=["financial"]),
    Field(name="insurance", type="INTEGER", desc="보험", mode="NULLABLE", path=["insurance"]),
    Field(name="mutualFund", type="INTEGER", desc="투신(사모)", mode="NULLABLE", path=["mutualFund"]),
    Field(name="bank", type="INTEGER", desc="은행", mode="NULLABLE", path=["bank"]),
    Field(name="otherInstitutions", type="INTEGER", desc="기타금융기관", mode="NULLABLE", path=["otherInstitutions"]),
    Field(name="pensionFund", type="INTEGER", desc="연기금등", mode="NULLABLE", path=["pensionFund"]),
    Field(name="others", type="INTEGER", desc="기타법인", mode="NULLABLE", path=["others"]),
)


NAVER_INVESTOR_INFO = lambda: Info(
    query = NAVER_INVESTOR_QUERY(),
    investor = NAVER_INVESTOR_SCHEMA(),
)
