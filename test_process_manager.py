import asyncio
import time
from typing import NoReturn, Optional
from unittest import TestCase

from module.crawling_data.async_crawling_data import AsyncCrawlingData
from module.need_crawling_fund.get_fund_by_web import GetNeedCrawledFundByWeb4Test
from module.need_crawling_fund.get_fund_by_web import GetNeedCrawledFundByWeb4_Assign
from module.save_result.save_result_2_file import SaveResult2File
from process_manager import NeedCrawledFundModule, CrawlingDataModule, FundCrawlingResult, SaveResultModule, \
    TaskManager


class SimpleTestTaskManager(TestCase):
    """
    简单测试, 验证下整个链路是否符合预期, 有没有漏水的地方
    """

    class TestNeedCrawledFundModule(NeedCrawledFundModule):

        def init_generator(self):
            self.total = 100
            self.task_generator = (NeedCrawledFundModule.NeedCrawledOnceFund(str(i), str(i)) for i in range(self.total))

    class TestCrawlingDataModule(CrawlingDataModule):
        def __init__(self):
            self._result_list: list[FundCrawlingResult] = list()
            self._shutdown = False

        def shutdown(self):
            self._shutdown = True

        def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund):
            print("do_crawling")
            # 模拟从队列中取结果时的block
            while len(self._result_list) > 0:
                time.sleep(0.1)

            self._result_list.append(FundCrawlingResult(task.code, task.name))

        def has_next_result(self) -> bool:
            return not (self._shutdown and not self._result_list)

        def get_an_result(self) -> Optional[FundCrawlingResult]:
            print("get_an_result")
            # 模拟从队列中取结果时的block
            max_iter = 10
            while not self._result_list and max_iter > 0:
                max_iter -= 1
                time.sleep(0.1)

            result = self._result_list.pop()
            return result

    class TestSaveResultModule(SaveResultModule):

        def save_result(self, result: FundCrawlingResult) -> NoReturn:
            print(f'the result is {result.fund_info_dict}')

    def test_run(self):
        manager = TaskManager(SimpleTestTaskManager.TestNeedCrawledFundModule()
                              , SimpleTestTaskManager.TestCrawlingDataModule()
                              , SimpleTestTaskManager.TestSaveResultModule())
        asyncio.run(manager.run())


class SmokeTestTaskManager(TestCase):
    """
    冒烟测试, 小批量爬取基金信息, 主要用于验证数据的爬取和清洗逻辑
    """

    def test_run(self):
        GetNeedCrawledFundByWeb4Test.test_case_num = 100
        manager = TaskManager(GetNeedCrawledFundByWeb4Test()
                              , AsyncCrawlingData()
                              , SaveResult2File())
        manager.run()

if __name__ == '__main__':
    
    GetNeedCrawledFundByWeb4_Assign.fund_list = [{"code":"001592","name":"天弘创业板ETF联接A"},{"code":"001879","name":"长城创业板指数增强A"},{"code":"002656","name":"南方创业板ETF联接A"},{"code":"003765","name":"广发创业板ETF联接A"},{"code":"005390","name":"工银创业板ETF联接A"},{"code":"005873","name":"建信创业板ETF联接A"},{"code":"006248","name":"华夏创业板ETF联接A"},{"code":"007664","name":"永赢创业板指数A"},{"code":"009012","name":"平安创业板ETF联接A"},{"code":"009046","name":"西藏东财创业板A"},{"code":"009981","name":"万家创业板指数增强A"},{"code":"010749","name":"浙商创业板指数增强A"},{"code":"010785","name":"博时创业板A"},{"code":"012116","name":"中银证券创业板ETF联接A"},{"code":"012179","name":"浦银安盛创业板ETF联接A"},{"code":"012900","name":"招商创业板指数增强A"},{"code":"015794","name":"天弘创业板指数增强A"},{"code":"018370","name":"华夏创业板指数增强A"},{"code":"161613","name":"融通创业板指数增强AB"},{"code":"050021","name":"博时创业板ETF联接A"},{"code":"110026","name":"易方达创业板ETF联接A"},{"code":"159675","name":"嘉实创业板增强策略ETF"},{"code":"159676","name":"富国创业板增强策略ETF"},{"code":"159808","name":"融通创业板ETF"},{"code":"159810","name":"浦银安盛创业板ETF"},{"code":"159821","name":"中银证券创业板ETF"},{"code":"159908","name":"博时创业板ETF"},{"code":"159915","name":"易方达创业板ETF"},{"code":"159948","name":"南方创业板ETF"},{"code":"159952","name":"广发创业板ETF"},{"code":"159956","name":"建信创业板ETF"},{"code":"159957","name":"华夏创业板ETF"},{"code":"159958","name":"工银创业板ETF"},{"code":"159964","name":"平安创业板ETF"},{"code":"159971","name":"富国创业板ETF"},{"code":"159977","name":"天弘创业板ETF"},{"code":"161022","name":"富国创业板指数A"}]
    # GetNeedCrawledFundByWeb4_Assign.fund_list = [{"code":"159676","name":"富国创业板增强策略ETF"}]
    manager = TaskManager(GetNeedCrawledFundByWeb4_Assign()
                            , AsyncCrawlingData()
                            , SaveResult2File())
    manager.run()
