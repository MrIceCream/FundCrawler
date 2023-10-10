"""
将爬取结果 保存到csv文件
"""
import os.path
from csv import DictWriter
from typing import NoReturn

from process_manager import SaveResultModule, FundCrawlingResult


class SaveResult2File(SaveResultModule):
    default_restval = 'None'
    result_file_path = './result/'
    result_file_name = 'result.csv'

    def __init__(self):
        fieldnames = [header.value for header in FundCrawlingResult.Header]

        if not os.path.exists(self.result_file_path):
            os.makedirs(self.result_file_path)

        self._file = open(self.result_file_path + self.result_file_name, 'w', newline='', encoding='utf-8')
        self._writer: DictWriter = DictWriter(self._file, fieldnames=fieldnames, restval=self.default_restval, delimiter='\t')

        self._writer.writeheader()

    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        row = {header.value: value if value else self.default_restval for header, value in
               result.fund_info_dict.items()}
        
        size = float(row[FundCrawlingResult.Header.FUND_SIZE])
        if size > 2 and size < 10:
            self._writer.writerow(row)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()
