import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

APPKEY = '63E070E10010'
APPSECRET = '41686E9EE9E48448CD15D0FA0666A8E2'

URL_TOKEN_B2B = 'https://b2b-api.10jqka.com.cn/gateway/service-mana/app/login-appkey'


class TokenUtil(object):

    def __init__(self):
        self.APPKEY = APPKEY
        self.APPSECRET = APPSECRET

    def set_APPKEY(self, APPKEY, APPSECRET):
        self.APPKEY = APPKEY
        self.APPSECRET = APPSECRET

    @staticmethod
    def get_token(self):
        result = requests.get(URL_TOKEN_B2B, params={'appkey': self.APPKEY, 'appSecret': self.APPSECRET})
        data = json.loads(result.text)
        if 0 != data.get('flag', -1):
            logger.error('鉴权失败: %s.' % data.get("msg"))
        else:
            return data.get('data').get('access_token')

