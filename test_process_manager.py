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
    # 沪深300
    # GetNeedCrawledFundByWeb4_Assign.fund_list = [{'code':'159673','name':'鹏华沪深300交易型开放式指数证券投资基金'},{'code':'510370','name':'兴业沪深300交易型开放式指数证券投资基金'},{'code':'515130','name':'博时沪深300交易型开放式指数证券投资基金'},{'code':'515380','name':'泰康沪深300交易型开放式指数证券投资基金'},{'code':'515350','name':'民生加银沪深300交易型开放式指数证券投资基金'},{'code':'515390','name':'华安沪深300交易型开放式指数证券投资基金'},{'code':'515330','name':'天弘沪深300交易型开放式指数证券投资基金'},{'code':'515310','name':'汇添富沪深300交易型开放式指数证券投资基金'},{'code':'515660','name':'国联安沪深300交易型开放式指数证券投资基金'},{'code':'515360','name':'方正富邦沪深300交易型开放式指数证券投资基金'},{'code':'510350','name':'工银瑞信沪深300交易型开放式指数证券投资基金'},{'code':'510380','name':'国寿安保沪深300交易型开放式指数证券投资基金'},{'code':'510390','name':'平安大华沪深300交易型开放式指数证券投资基金'},{'code':'510360','name':'广发沪深300交易型开放式指数证券投资基金'},{'code':'510310','name':'易方达沪深300交易型开放式指数发起式证券投资基金'},{'code':'159925','name':'南方开元沪深300交易型开放式指数证券投资基金'},{'code':'510330','name':'华夏沪深300交易型开放式指数证券投资基金'},{'code':'159919','name':'嘉实沪深300交易型开放式指数证券投资基金'},{'code':'510300','name':'华泰柏瑞沪深300交易型开放式指数证券投资基金'}]
    # 中证500
    GetNeedCrawledFundByWeb4_Assign.fund_list = [{'code':'561350','name':'国泰中证500交易型开放式指数证券投资基金'},{'code':'515530','name':'泰康中证500交易型开放式指数证券投资基金'},{'code':'159820','name':'天弘中证500交易型开放式指数证券投资基金'},{'code':'510570','name':'兴业中证500交易型开放式指数证券投资基金'},{'code':'515190','name':'中银证券中证500交易型开放式指数证券投资基金'},{'code':'159982','name':'鹏华中证500交易型开放式指数证券投资基金'},{'code':'515550','name':'中融中证500交易型开放式指数证券投资基金'},{'code':'510530','name':'工银瑞信中证500交易型开放式指数证券投资基金'},{'code':'159968','name':'博时中证500交易型开放式指数证券投资基金'},{'code':'510550','name':'方正富邦中证500交易型开放式指数证券投资基金'},{'code':'510590','name':'平安大华中证500交易型开放式指数证券投资基金'},{'code':'510580','name':'易方达中证500交易型开放式指数证券投资基金'},{'code':'510560','name':'国寿安保中证500交易型开放式指数证券投资基金'},{'code':'512510','name':'华泰柏瑞中证500交易型开放式指数证券投资基金'},{'code':'512500','name':'华夏中证500交易型开放式指数证券投资基金'},{'code':'159935','name':'景顺长城中证500交易型开放式指数证券投资基金'},{'code':'510510','name':'广发中证500交易型开放式指数证券投资基金'},{'code':'159922','name':'嘉实中证500交易型开放式指数证券投资基金'},{'code':'510500','name':'中证500交易型开放式指数证券投资基金'}]
    # 创业板指
    # GetNeedCrawledFundByWeb4_Assign.fund_list = [{"code":"001592","name":"天弘创业板ETF联接A"},{"code":"001879","name":"长城创业板指数增强A"},{"code":"002656","name":"南方创业板ETF联接A"},{"code":"003765","name":"广发创业板ETF联接A"},{"code":"005390","name":"工银创业板ETF联接A"},{"code":"005873","name":"建信创业板ETF联接A"},{"code":"006248","name":"华夏创业板ETF联接A"},{"code":"007664","name":"永赢创业板指数A"},{"code":"009012","name":"平安创业板ETF联接A"},{"code":"009046","name":"西藏东财创业板A"},{"code":"009981","name":"万家创业板指数增强A"},{"code":"010749","name":"浙商创业板指数增强A"},{"code":"010785","name":"博时创业板A"},{"code":"012116","name":"中银证券创业板ETF联接A"},{"code":"012179","name":"浦银安盛创业板ETF联接A"},{"code":"012900","name":"招商创业板指数增强A"},{"code":"015794","name":"天弘创业板指数增强A"},{"code":"018370","name":"华夏创业板指数增强A"},{"code":"161613","name":"融通创业板指数增强AB"},{"code":"050021","name":"博时创业板ETF联接A"},{"code":"110026","name":"易方达创业板ETF联接A"},{"code":"159675","name":"嘉实创业板增强策略ETF"},{"code":"159676","name":"富国创业板增强策略ETF"},{"code":"159808","name":"融通创业板ETF"},{"code":"159810","name":"浦银安盛创业板ETF"},{"code":"159821","name":"中银证券创业板ETF"},{"code":"159908","name":"博时创业板ETF"},{"code":"159915","name":"易方达创业板ETF"},{"code":"159948","name":"南方创业板ETF"},{"code":"159952","name":"广发创业板ETF"},{"code":"159956","name":"建信创业板ETF"},{"code":"159957","name":"华夏创业板ETF"},{"code":"159958","name":"工银创业板ETF"},{"code":"159964","name":"平安创业板ETF"},{"code":"159971","name":"富国创业板ETF"},{"code":"159977","name":"天弘创业板ETF"},{"code":"161022","name":"富国创业板指数A"}]
    
    manager = TaskManager(GetNeedCrawledFundByWeb4_Assign()
                            , AsyncCrawlingData()
                            , SaveResult2File())
    manager.run()
