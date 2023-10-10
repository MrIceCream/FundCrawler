import re
from string import Template
from typing import NoReturn

from requests import Response

from module.crawling_data.data_mining.data_cleaning_strategy import DataCleaningStrategy
from process_manager import FundCrawlingResult


class ManagerStrategy(DataCleaningStrategy):
    """
    解析基金经理信息
    """
    url_template = Template('http://fund.eastmoney.com/manager/$manager_code.html')

    fund_total_work_time_pattern = re.compile(r'累计任职时间：</span>(.+?)<br />')
    fund_total_manager_size_pattern = re.compile(r"总规模.+?\r\n.+redText\'>(.+?)<")
    fund_big_size_exp_pattern = re.compile(r"基金经理简介：</span>(.+?)\r\n")
    fund_manager_name_pattern = re.compile(r"基金经理：(.+?)<")

    def build_url(self, fund_code: str, manager_code: str) -> str:
        return self.url_template.substitute(manager_code=manager_code)

    def fill_result(self, response: Response, result: FundCrawlingResult) -> NoReturn:
        page_text = response.text

        fund_total_work_time = self.fund_total_work_time_pattern.search(page_text)
        if fund_total_work_time:
            result.fund_info_dict[FundCrawlingResult.Header.TOTAL_WORK_TIME] = fund_total_work_time.group(1)

        fund_total_manager_size = self.fund_total_manager_size_pattern.search(page_text)
        if fund_total_manager_size:
            result.fund_info_dict[FundCrawlingResult.Header.TOTAL_MANAGER_SIZE] = fund_total_manager_size.group(1)

        fund_big_size_exp = self.fund_big_size_exp_pattern.search(page_text)
        if fund_big_size_exp:
            per_info = fund_big_size_exp.group(1)
            result.fund_info_dict[FundCrawlingResult.Header.BIG_SIZE_EXP] = "✅" if "量化" in per_info else ""

        fund_manager_name = self.fund_manager_name_pattern.search(page_text)
        if fund_manager_name:
                result.fund_info_dict[FundCrawlingResult.Header.MANAGER_INFO] = fund_manager_name.group(1)
        result.fund_info_dict[FundCrawlingResult.Header.MANAGER_INFO] += "," + response.url
