import logging
from iFinDPy import *
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, DATETIME, TEXT
from concept_helper import *

# 设置日志
logging.basicConfig(filename='concept_running.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')



def update_stock_concept_data(date):
    """
    date: str 日期
    stock_concept: DataFrame 股票概念映射表
    """
    try:
        new_stock_concept = stock_concept_data(date)
        new_stock_concept.rename(columns={'thscode': 'wind_code', 'ths_stock_short_name_stock':'sec_name', 
                                        'ths_the_concept_stock': 'concepts', 'concept_num': 'concept_num', 'date': 'tradedate'}, inplace=True)
        new_stock_concept = new_stock_concept[['tradedate', 'wind_code', 'sec_name', 'concept_num', 'concepts']]
        logging.info('股票概念映射表更新成功')
    except Exception as e:
        logging.error('股票概念映射表更新失败: {}'.format(str(e)))
        new_stock_concept = None
    try:
        new_concept_stock = concept_stock_data(new_stock_concept, date)
        new_concept_stock.rename(columns={'date': 'tradedate', 'concept':'concepts', 
                                        'stock_num': 'stock_num', 'stock_code': 'stock_codes'}, inplace=True)
        new_concept_stock = new_concept_stock[['tradedate', 'concepts', 'stock_num', 'stock_codes']]
        logging.info('概念股票映射表更新成功')
    except Exception as e:
        logging.error('概念股票映射表更新失败: {}'.format(str(e)))
        new_concept_stock = None
    return new_stock_concept, new_concept_stock

# 主函数
def job(date='2023-07-25'):                     
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
    logging.info('开始执行任务: {}'.format(date))
    if thslogin():
        try:
            new_stock_concept, new_concept_stock = update_stock_concept_data(date)
            
        except Exception as e:
            logging.error('更新股票概念映射表或概念股票映射表错误: {}'.format(str(e)))
            new_stock_concept = None
            new_concept_stock = None

        if new_stock_concept is not None and new_concept_stock is not None:
            new_stock_concept_dict = {col: VARCHAR(length=30) for col in new_stock_concept.columns}
            new_stock_concept_dict['tradedate'] = DATETIME(6)
            new_stock_concept_dict['wind_code'] = VARCHAR(length=9)
            new_stock_concept_dict['concepts'] = VARCHAR(length=300)
            new_stock_concept.to_sql('stock_concept', engine, if_exists='append', index=False, dtype=new_stock_concept_dict)
            new_concept_stock_dict = {col: VARCHAR(length=30) for col in new_stock_concept.columns}
            new_concept_stock_dict['tradedate'] = DATETIME(6)
            new_concept_stock_dict['stock_codes'] = TEXT
            new_concept_stock.to_sql('concept_stock', engine, if_exists='append', index=False, dtype=new_concept_stock_dict)
            logging.info('{} 写入数据库成功'.format(date))
        else:
            logging.info('{} 没有数据被写入数据库'.format(date))
    else:
        logging.info('{} 任务没有被执行因为登录失败'.format(date))


if __name__ == '__main__':
    job('2023-07-25')