import requests
import json
import pandas as pd
import logging
import sys
from concept_factor.dependencies import TokenUtil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

APPKEY = '63E070E10010'
APPSECRET = '41686E9EE9E48448CD15D0FA0666A8E2'

URL_CONCEPT_ALL = "https://b2b-api.10jqka.com.cn/gateway/arsenal/eventdriven.ifind-db/data_supply/concept/all";
URL_CONCEPT_SYMBOL_INFO = "https://b2b-api.10jqka.com.cn/gateway/arsenal/eventdriven.ifind-db/data_supply/concept/symbol/info";
URL_CONCEPT_HT_INDEX = "https://b2b-api.10jqka.com.cn/gateway/arsenal/eventdriven.ifind-db/data_supply/concept/ht/index";
URL_CONCEPT_MARK_POINT_NEWS = "https://b2b-api.10jqka.com.cn/gateway/arsenal/eventdriven.ifind-db/data_supply/concept/mark_point/news";

# 有效期24h
A = TokenUtil.TokenUtil()
A.set_APPKEY(APPKEY, APPSECRET)
ACCESS_TOKEN_VALUE = A.get_token(A)


class B2BRequest(object):

    def __init__(self):
        self.APPKEY = APPKEY
        self.ACCESS_TOKEN_VALUE = ACCESS_TOKEN_VALUE

    def set_token(self, APPKEY, ACCESS_TOKEN_VALUE):
        self.APPKEY = APPKEY
        self.ACCESS_TOKEN_VALUE = ACCESS_TOKEN_VALUE

    @staticmethod
    def get(url, params):
        result = requests.get(url, params=params, headers={'open-authorization': 'Bearer' + ACCESS_TOKEN_VALUE})
        if result.status_code != 200:
            logger.error("请求错误: url=[%s] params=[%s] res=[%s]", url, params, result)
            raise Exception("请求错误")
        return result.text

    @staticmethod
    def post(url, payload):
        result = requests.post(url, data=payload, headers={'open-authorization': 'Bearer' + ACCESS_TOKEN_VALUE})
        if result.status_code != 200:
            logger.error("请求错误: url=[%s] payload=[%s]", url, payload)
            raise Exception("请求错误")
        return result.text


class DataHandler(object):

    @staticmethod
    def handle(res, url, name, param):
        data = json.loads(res)
        if 0 != data.get('flag', 0):
            logger.error('鉴权失败: %s.' % data.get("msg"))
            return str(data.get("flag")) + data.get("msg")
        if data.get("status_code") != 0:
            logger.log(name + "失败：url=[%s],msg=[%s],param=[%s]", name, url, data.get("status_msg"), param)
            raise Exception(name + "失败")
        return pd.DataFrame(data.get("data"))

class ConceptEvent(object):

    """
    @:desc 获取同花顺所有概念
    """
    @staticmethod
    def get_concept_all():
        res = B2BRequest.get(URL_CONCEPT_ALL, {})
        return DataHandler.handle(res, URL_CONCEPT_ALL, "获取同花顺所有概念", "")

    """
    @:desc 获取同花顺概念标的
    @:id 概念id
    """
    @staticmethod
    def get_concept_symbol(id):
        p = {"id": id}
        res = B2BRequest.get(URL_CONCEPT_SYMBOL_INFO, p)
        data = json.loads(res)
        return data.get("data")

    """
    @:desc 获取同花顺概念热度数据
    @:id 概念id
    @:dateRange 默认1year 全部【all】 一年【1year】，类推 一月【1month】，类推 一周【1week】，类推 指定日期【2020-11-14~2020-12-14】 实时【real】
    """
    @staticmethod
    def get_concept_ht_index(id,date_range="1week"):
        p = {"id": id, "dateRange": date_range}
        res = B2BRequest.get(URL_CONCEPT_HT_INDEX, p)
        return DataHandler.handle(res,URL_CONCEPT_HT_INDEX, "获取同花顺概念热度数据", p)


    """
    @:desc 获取同花顺概念异动新闻
    @:id 概念id
    """
    @staticmethod
    def get_concept_mark_point_news(id):
        p = {"id": id}
        res = B2BRequest.get(URL_CONCEPT_MARK_POINT_NEWS,p)
        return DataHandler.handle(res, URL_CONCEPT_MARK_POINT_NEWS, "获取同花顺概念异动新闻", p)

