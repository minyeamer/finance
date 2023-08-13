from gscraper.base import Parser
from gscraper.cast import cast_float, cast_int
from gscraper.date import now
from gscraper.map import fill_array, filter_map, re_get
from gscraper.parse import select_text

from typing import Dict
from bs4 import BeautifulSoup
import json

NAVER = "naver"
KST = "Asia/Seoul"

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
        info = {"code":code, "stockType":"stock", "updateDate":now(tzinfo=KST).date(), "updateTime":now(tzinfo=KST)}
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
        info = {"code":code, "stockType":"etf", "updateDate":now(tzinfo=KST).date(), "updateTime":now(tzinfo=KST)}
        info["name"] = data.get("CMP_KOR")
        info["url"] = data.get("URL")
        info["en_name"] = data.get("CMP_ENG")
        info["index"] = data.get("BASE_IDX_NM_KOR")
        info["manager"] = data.get("ISSUE_NM_KOR")
        info["etfType"] = data.get("ETF_TYP_SVC_NM")
        info["totalPay"] = cast_float(data.get("TOT_PAY"), None)
        return info
