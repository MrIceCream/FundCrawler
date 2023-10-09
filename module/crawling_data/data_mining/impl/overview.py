import re
from string import Template
from typing import NoReturn

from requests import Response

from module.crawling_data.data_mining.data_cleaning_strategy_factory import DataCleaningStrategy
from module.crawling_data.data_mining.impl.constants import number_in_eng
from process_manager import FundCrawlingResult


class OverviewStrategy(DataCleaningStrategy):
    """
    解析基金的基本概况
    """
    url_template = Template('http://fundf10.eastmoney.com/jbgk_$fund_code.html')

    fund_target_pattern = re.compile(r'跟踪标的</th><td>(.+?)</td>')
    fund_type_pattern = re.compile(r'基金类型</th><td>(.+?)</td>')
    fund_size_pattern = re.compile(fr'资产规模</th><td>({number_in_eng})亿元')
    fund_company_pattern = re.compile(r'基金管理人</th><td><a.*?">(.+?)</a></td>')
    fund_value_pattern = re.compile(fr'单位净值.*?：[\s\S]*?({number_in_eng})\s')
    fund_price1_pattern = re.compile(r'管理费率</th><td>(.+?)%')
    fund_price2_pattern = re.compile(r'托管费率</th><td>(.+?)%')
    fund_price3_pattern = re.compile(r'销售服务费率</th><td>(.+?)%')

    def build_url(self, fund_code: str) -> str:
        return self.url_template.substitute(fund_code=fund_code)

    def fill_result(self, response: Response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_target = self.fund_target_pattern.search(page_text)
        if fund_target:
            result.fund_info_dict[FundCrawlingResult.Header.FUND_TARGET] = fund_target.group(1)

        fund_kind_result = self.fund_type_pattern.search(page_text)
        # if fund_kind_result:
        #     result.fund_info_dict[FundCrawlingResult.Header.FUND_TYPE] = fund_kind_result.group(1)

        fund_size_result = self.fund_size_pattern.search(page_text)
        if fund_size_result:
            # 1,179.10 亿元
            fund_size = fund_size_result.group(1).replace(',', '')
            result.fund_info_dict[FundCrawlingResult.Header.FUND_SIZE] = fund_size

        fund_company_result = self.fund_company_pattern.search(page_text)
        # if fund_company_result:
        #     result.fund_info_dict[FundCrawlingResult.Header.FUND_COMPANY] = fund_company_result.group(1)

        fund_value_result = self.fund_value_pattern.search(page_text)
        # if fund_value_result:
        #     result.fund_info_dict[FundCrawlingResult.Header.FUND_VALUE] = fund_value_result.group(1)

        fund_price1_result = self.fund_price1_pattern.search(page_text)
        fund_price2_result = self.fund_price2_pattern.search(page_text)
        fund_price3_result = self.fund_price3_pattern.search(page_text)
        total_price = 0
        if fund_price1_result:
            total_price += float(fund_price1_result.group(1))
        if fund_price2_result:
            total_price += float(fund_price2_result.group(1))
        if fund_price3_result and len(fund_price3_result.group(1)) < 5:
            total_price += float(fund_price3_result.group(1))
        result.fund_info_dict[FundCrawlingResult.Header.FUND_PRICE] = round(total_price, 2)

