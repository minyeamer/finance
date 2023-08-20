from gscraper.base import Parser, log_results
from gscraper.cast import cast_float, cast_int, cast_date
from gscraper.date import now, get_date
from gscraper.map import fill_array, filter_map, re_get, filter_data
from gscraper.parse import select_text

from typing import Dict, List
from bs4 import BeautifulSoup
from io import StringIO
import json
import pandas as pd

NAVER = "naver"
KST = "Asia/Seoul"

INDEX_TYPES = {"KOSPI":"코스피", "KOSDAQ":"코스탁", "FUTURES":"선물"}
INSTITUTIONS = ["금융투자", "보험", "투신(사모)", "은행", "기타금융기관", "연기금등"]
INVESTORS = ["개인", "외국인", "기관계"]+INSTITUTIONS+["기타법인"]

drop_colon = lambda s: s.split(" : ")[-1] if isinstance(s, str) and " : " in s else s


class NaverReportParser(Parser):
    operation = "naverReport"

    def parse(self, response: str, code=str(), stockType=str(), filter=list(), **kwargs) -> Dict:
        info = self.parse_company(response, code) if stockType == "company" else self.parse_etf(response, code)
        return filter_map(info, filter=filter)

    def parse_company(self, response: str, code=str(), **kwargs) -> Dict:
        source = BeautifulSoup(response, "html.parser")
        data = select_text(source, "td.cmp-table-cell > dl > dt.line-left", many=True)
        return self.map_company(code, *fill_array(data, 7))

    def map_company(self, code: str, name: str, sector: str, industry: str,
                    bps: str, per: str, sper: str, pbr: str, **kwargs) -> Dict:
        info = {"code":code, "stockType":"stock", "updateDate":now().date(), "updateTime":now()}
        info["name"] = name
        info["sector"] = drop_colon(sector)
        info["industry"] = drop_colon(industry)
        info["BPS"] = cast_int(bps, None)
        info["PER"] = cast_float(per, None)
        info["SectorPER"] = cast_float(sper, None)
        info["PBR"] = cast_float(pbr, None)
        return info

    def parse_etf(self, response: str, code=str(), **kwargs) -> Dict:
        data = json.loads(re_get("var summary_data = (\{[^}]*\});", response))
        return self.map_etf(data, code)

    def map_etf(self, data: Dict, code=str(), **kwargs) -> Dict:
        info = {"code":code, "stockType":"etf", "updateDate":now().date(), "updateTime":now()}
        info["name"] = data.get("CMP_KOR")
        info["url"] = data.get("URL")
        info["en_name"] = data.get("CMP_ENG")
        info["index"] = data.get("BASE_IDX_NM_KOR")
        info["manager"] = data.get("ISSUE_NM_KOR")
        info["etfType"] = data.get("ETF_TYP_SVC_NM")
        info["totalPay"] = cast_float(data.get("TOT_PAY"), None)
        return info


class NaverInvestorParser(Parser):
    operation = "naverInvestor"

    def parse(self, response: str, indexType=str(), startDate=None,
                filter=list(), **kwargs) -> List[Dict]:
        raw = StringIO(response)
        data = pd.read_html(raw)[0]
        data.columns = ["날짜"]+INVESTORS
        results = self.map_investor(data, indexType, startDate, filter=filter, **kwargs)
        log_results(results, indexType=indexType)
        return results

    def map_investor(self, data: pd.DataFrame, indexType=str(), startDate=None,
                    filter=list(), **kwargs) -> List[Dict]:
        data = data[data["날짜"].notna()].copy()
        data["날짜"] = data["날짜"].apply(cast_date)
        data["대상"] = INDEX_TYPES.get(indexType)
        for column in INVESTORS:
            data[column] = data[column].apply(cast_int)
        startDate = get_date(startDate, default=None)
        if startDate: data = data[data["날짜"]>=startDate]
        return filter_data(data, filter=filter, return_type="records")
