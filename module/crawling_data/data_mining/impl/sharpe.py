import re
from string import Template
from typing import NoReturn

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from process_manager import FundCrawlingResult


class MetricsStrategy(DataCleaningStrategy):
    """
    解析标准差和夏普比率
    """
    url_template = Template('http://fundf10.eastmoney.com/tsdata_$fund_code.html')

    fund_standard_deviation_pattern = re.compile(r'标准差.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')
    fund_sharpe_ratio_pattern = re.compile(r'夏普比率.+?\'>(.+?)<.+?\'>(.+?)<.+?\'>(.+?)<')
    fund_track_offset_ratio_pattern = re.compile(r'同类平均跟踪误差<.+>\r\n.+\r\n.+\r\n.+?</td><td >(.+?)<')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_standard_deviation = self.fund_standard_deviation_pattern.search(page_text)
        if fund_standard_deviation:
            standard_deviation = fund_standard_deviation.group(3)
            # -- 代表无此数据
            standard_deviation = None if standard_deviation == '--' else standard_deviation
            # result.fund_info_dict[FundCrawlingResult.Header.STANDARD_DEVIATION_THREE_YEARS] = standard_deviation
        
        fund_sharpe_ratio = self.fund_sharpe_ratio_pattern.search(page_text)
        if fund_sharpe_ratio:
            sharpe = fund_sharpe_ratio.group(3)
            # -- 代表无此数据
            sharpe = None if sharpe == '--' else sharpe
            # result.fund_info_dict[FundCrawlingResult.Header.SHARPE_THREE_YEARS] = sharpe

        fund_track_offset_ratio = self.fund_track_offset_ratio_pattern.search(page_text)
        if fund_track_offset_ratio:
            track_offset = fund_track_offset_ratio.group(1)
            result.fund_info_dict[FundCrawlingResult.Header.TRACK_OFFSET] = track_offset
