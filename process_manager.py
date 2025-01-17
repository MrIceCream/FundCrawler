"""
爬取核心
对爬取过程的管理
"""
import logging
import os
from abc import abstractmethod, ABC
from collections.abc import Generator
from datetime import datetime
from enum import unique, StrEnum
from threading import Thread
from time import sleep
from typing import NoReturn, Optional


class NeedCrawledFundModule(ABC):
    """
    基金爬取任务模块
    通过生成器逐个给出 需要爬取的基金
    """

    class NeedCrawledOnceFund:
        """
        需要爬取的 单个基金信息
        """

        def __init__(self, code: str, name: str):
            self.code = code
            self.name = name

    def __init__(self):
        self.total = None
        self.task_generator: Optional[Generator[NeedCrawledFundModule.NeedCrawledOnceFund]] = None

        self.init_generator()

    @abstractmethod
    def init_generator(self) -> NoReturn:
        """
        初始化 生成器
        """
        return NotImplemented


class FundCrawlingResult:
    """
    基金的最终爬取结果定义
    """

    @unique
    class Header(StrEnum):
        """
        结果key
        """
        FUND_SIMPLE_NAME = '基金简称',
        FUND_CODE = '基金代码',
        # FUND_TYPE = '基金类型',
        FUND_SIZE = '资产规模(亿)',
        # FUND_COMPANY = '基金管理人',
        # FUND_VALUE = '基金净值',
        TRACK_OFFSET = '跟踪误差',
        FUND_PRICE = '三项费率',
        # 兼容带新场景，A+B -> B -> B+C，此时基金经理为时长最长的B，对应的任职时间为 这三段 B连续任职的任职时间
        # FUND_MANAGER = '基金经理(最近连续最长任职)',
        # DATE_OF_APPOINTMENT = '基金经理的上任时间',
        # STANDARD_DEVIATION_THREE_YEARS = '近三年标准差',
        # SHARPE_THREE_YEARS = '近三年夏普',
        # THREE_YEARS_INCREASE = '近三年涨幅',
        # FIVE_YEARS_INCREASE = '近五年涨幅'

    def __init__(self, fund_code: str, fund_name: str):
        self.fund_info_dict = {FundCrawlingResult.Header.FUND_CODE: fund_code,
                               FundCrawlingResult.Header.FUND_SIMPLE_NAME: fund_name}


class CrawlingDataModule(ABC):
    """
    数据爬取模块
    包括数据的下载和清洗
    """

    @abstractmethod
    def do_crawling(self, task: NeedCrawledFundModule.NeedCrawledOnceFund) -> NoReturn:
        """
        提交任务
        需要有任务堆积时的阻塞, 以便可以将时间片让出来 处理结果
        """
        return NotImplemented

    @abstractmethod
    def has_next_result(self) -> bool:
        """
        请求已经全部处理完, 且结果都被取出了
        在shutdown后调用
        """
        return NotImplemented

    @abstractmethod
    def get_an_result(self) -> Optional[FundCrawlingResult]:
        """
        (阻塞, 有超时)获取一个处理好的结果
        数据爬取尽量保证成功, 实在失败时 爬取数据为None, 所以不期望的异常
        可以认为只存在于 数据解析 部分, 需要防止一个任务失败导致全部失败
        """
        return NotImplemented

    @abstractmethod
    def shutdown(self):
        """
        请求已经全部传递完了
        """
        return NotImplemented


class SaveResultModule(ABC):
    """
    基金数据的保存模块
    """

    @abstractmethod
    def save_result(self, result: FundCrawlingResult) -> NoReturn:
        """
        爬取结果的保存
        """
        return NotImplemented

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class TaskManager:
    """
    爬取核心
    """

    def __init__(self, need_crawled_fund_module: NeedCrawledFundModule, crawling_data_module: CrawlingDataModule,
                 save_result_module: SaveResultModule, log_level=logging.DEBUG):
        """
        :param need_crawled_fund_module: 负责给出 基金爬取任务
        :param crawling_data_module: 负责 数据爬取和清洗
        :param save_result_module: 负责 数据保存
        """
        self._need_crawled_fund_module = need_crawled_fund_module
        self._crawling_data_module = crawling_data_module
        self._save_result_module = save_result_module

        log_file_path = './log/'
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)
        logging.basicConfig(filename=log_file_path + 'process.text', encoding='utf-8', level=log_level, filemode='w',
                            format='%(asctime)s %(message)s')
        logging.info(f"需要爬取的基金总数:{self._need_crawled_fund_module.total}")

        self._cur_finished_task_count = 0
        self._all_task_finished = False

    def get_task_and_crawling(self):
        generator = self._need_crawled_fund_module.task_generator

        while True:
            try:
                task: NeedCrawledFundModule.NeedCrawledOnceFund = next(generator)
            except StopIteration:
                break
            self._crawling_data_module.do_crawling(task)

        self._crawling_data_module.shutdown()

    def get_result_and_save(self):
        with self._save_result_module:
            while self._crawling_data_module.has_next_result():
                result: FundCrawlingResult = self._crawling_data_module.get_an_result()
                if result:
                    self._save_result_module.save_result(result)
                    self._cur_finished_task_count += 1

        self._all_task_finished = True

    def show_process(self):
        while not self._all_task_finished:
            logging.info(f"已爬取完成基金数:{self._cur_finished_task_count}")
            sleep(5)

    def run(self) -> NoReturn:
        """
        爬取主流程
        从 基金爬取任务模块 将任务传递给 数据爬取和清洗模块
        从 数据爬取和清洗模块 将结果传递给 数据保存模块
        两部分的任务都是阻塞的（主要会阻塞在 数据爬取和清洗）
        """
        start_time = datetime.now()

        thread1 = Thread(target=self.get_task_and_crawling)
        thread2 = Thread(target=self.get_result_and_save)
        thread3 = Thread(target=self.show_process)

        thread1.start()
        thread2.start()
        thread3.start()

        thread1.join()
        thread2.join()
        thread3.join()

        cur_time = datetime.now()
        logging.info(f"基金爬取完成 耗时{(cur_time - start_time).seconds}s")
