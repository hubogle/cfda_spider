# _*_coding:utf-8_*_
"""
爬取国家药品公开数据
"""
from selenium import webdriver
from urllib.parse import urlencode
from pybloom_live import BloomFilter
from bs4 import BeautifulSoup
import os
import re
import csv
import queue
import time
import threading


# 待爬取的数据
ITEM = [
    {
        "name": "药品数据",
        "csv_title": ["批准文号", "产品名称", "英文名称", "商品名", "剂型",
                      "规格", "生产单位", "生产地址", "产品类别", "批准日期",
                      "原批准文号", "药品本位码", "药品本位码备注"],
        "params": {
            'tableId': '25',
            'tableName': 'TABLE25',
            'tableView': '国产药品',
            'Id': None
        },
        "num": 200000   # 药品数据总数
    },
    {
        "name": "保健食品数据",
        "csv_title": ["保健食品广告批准文号", "申请人(广告主)", "广告有效期", "广告发布内容",
                      "批准文号", "备注", "地址", "邮编", "保健食品名称", "品牌名称", "广告类别", "时长"],
        "params": {
            "tableId": "29",
            "tableName": "TABLE29",
            "tableView": "保健食品广告",
            "Id": None,
        },
        "num": 200000
    },
    {
        "name": "医疗器械数据",
        "csv_title": ["注册证编号", "注册人名称", "注册人住所", "生产地址", "代理人名称",
                      "代理人住所", "产品名称", "型号丶规格", "结构及组成", "适用范围",
                      "其他内容", "备注", "批准日期", "有效期至", "产品标准"],
        "params": {
            "tableId": "26",
            "tableName": "TABLE26",
            "tableView": "国产器械",
            "Id": None,
        },
        "num":  200000,
    },
    {
        "name": "化妆品数据",
        "csv_title": ["产品名称", "产品类别", "生产企业", "生产企业地址", "批准文号", "批件状态",
                      "批准日期", "证件有效期", "卫生许可证号", "产品名称备注"],
        "params": {
            "tableId": "68",
            "tableName": "TABLE68",
            "tableView": "国产化妆品",
            "Id": None
        },
        "num": 200000
    },
    {
        "name": "进口化妆品数据",
        "csv_title": ["产品名称", "产品类别", "生产企业", "生产企业地址", "批准文号", "批件状态",
                      "批准日期", "证件有效期", "卫生许可证号", "产品名称备注"],
        "params": {
            "tableId": "69",
            "tableName": "TABLE69",
            "tableView": "进口化妆品",
            "Id": None
        },
        "num": 200000
    }
]


class CrawCFDA:
    """
    爬取CFDA数据
    """
    def __init__(self, item_dict=None):
        for key in ["name", "csv_title", "params", "num"]:
            if key not in item_dict:
                raise ValueError("{} not in item_dict.".format(key))
        # 请求需要的参数
        self.params = item_dict.get("params")
        self.csv_title = item_dict.get("csv_title")
        self.name = item_dict.get("name")
        self.num = item_dict["num"]
        if not isinstance(self.params, dict):
            raise ValueError("params must be dict.")
        if not isinstance(self.csv_title, list):
            raise ValueError("csv_title must be list.")
        if not isinstance(self.name, str):
            raise ValueError("name must be str.")
        if not isinstance(self.num, int):
            raise ValueError("num must be int.")

        self.row_num = len(self.csv_title)
        self.url_que = queue.Queue()
        self.data_que = queue.Queue()
        self.end = False

    @staticmethod
    def general_browser():
        """
        :return:
        """
        options = webdriver.ChromeOptions()
        options.add_argument('lang=zh_CN.UTF-8')
        options.add_argument(
            'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36"')
        browser = webdriver.Chrome(chrome_options=options)
        return browser

    def filter_url(self, url):
        """
        进行url去重处理，可能需要的请求数据过多，防止重复
        :param url:对url进行判断，看是否重复
        :return:
        """
        bloom_path = '{}.blm'.format(self.name)
        # 判断是否存在这个文件
        is_exist = os.path.exists(bloom_path)
        if is_exist:
            bf = BloomFilter.fromfile(open(bloom_path, 'rb'))
        else:
            # 新建一个，储存在内存中
            bf = BloomFilter(1000000, 0.01)
        if url in bf:
            return False
        # 不存在将url添加进去
        bf.add(url)
        bf.tofile(open(bloom_path, 'wb'))
        return True

    def run(self, thread_num=4):
        """
        pass
        :return:
        """
        print("正在初始化URL...")
        for i in range(1, self.num):
            self.params["Id"] = i
            url = 'http://app1.sfda.gov.cn/datasearch/face3/content.jsp?' + urlencode(self.params)
            self.url_que.put(url)
        print("初始化URL完成...")
        for i in range(thread_num):
            threading.Thread(target=self.parse).start()
        self.write_csv()

    def parse(self):
        """
        解析数据
        :return:
        """
        # 第一行开始
        browser = type(self).general_browser()
        pattern = re.compile('没有相关信息', re.S)
        start_row = 1
        end_row = self.row_num
        while not self.url_que.empty():
            url = self.url_que.get()
            # if not self.filter_url(url):    # 过滤URL
            #     continue
            browser.get(url)
            content = browser.page_source
            # 没有相关信息
            if re.search(pattern, content):
                continue
            browser.implicitly_wait(time_to_wait=1)
            item = []
            soup = BeautifulSoup(content, 'lxml')
            for tr in soup.find('tbody').find_all('tr')[start_row:end_row]:
                value = tr.find_all('td')[1].text
                item.append(value)
            self.data_que.put(item)
        self.end = True

    def write_csv(self):
        """
        写入csv文件
        :param filename:
        :return:
        """

        # csv头部信息
        f = open("{}.csv".format(self.name), "w", encoding="utf-8", newline='')
        csv_writer = csv.writer(f, dialect='excel')
        try:

            csv_writer.writerow(self.csv_title)
            count = 1
            while True:
                time.sleep(10)
                current_qsize = self.data_que.qsize()
                print("当前队列数据有:{}条".format(str(current_qsize)))
                while current_qsize > 0:
                    print("写入第{}条数据...".format(str(count)))
                    data = self.data_que.get()
                    csv_writer.writerow(data)
                    count += 1
                    current_qsize -= 1
                if self.end:
                    break
            print("写入数据完成...")

        except KeyboardInterrupt:
            print('用户强制退出')
        finally:
            f.close()



if __name__ == '__main__':
    for i in range(len(ITEM)):
        app = CrawCFDA(ITEM[i])
        app.run()
