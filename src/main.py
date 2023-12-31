'''
Author: Tangzhong-Tian 116010205@link.cuhk.edu.cn
Date: 2023-07-26 14:18:52
LastEditors: Tangzhong-Tian 116010205@link.cuhk.edu.cn
LastEditTime: 2023-08-18 13:18:37
FilePath: \Concept-Factor\src\main.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import logging
import sys
from iFinDPy import *
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DATE, FLOAT, INT, DATETIME
import datetime

sys.path.append(r'C:\Users\hazc\Desktop\Concept-Factor\dependencies')
import concept_helper as cp
from ConceptEvent import ConceptEvent

# 设置日志
logging.basicConfig(filename='concept_running.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


#################################################################################### 更新概念数据 ##################################################################
def update_concepts_status():
    """
    更新市场概念纳入剔除表
    """
    concepts_df = ConceptEvent().get_concept_all()[['id', 'name']]
    concepts_DF = pd.DataFrame()
    for _, concept in concepts_df.iterrows():
        concept_id = concept['id']
        concept_name = concept['name']
        concept_status = cp.get_concept_status(concept_id, concept_name)
        concepts_DF = pd.concat([concepts_DF, concept_status], axis=0, ignore_index=True)
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
    table_name = 'ths_concepts_status'
    data_dict = {'wind_code': VARCHAR(length=9), 'sec_name':VARCHAR(30),
            'valid': INT(), 'tradedate': DATE(), 'similarity': FLOAT(),
            'concept': VARCHAR(30),'status':VARCHAR(50), 'updatetime': DATETIME()}
    concepts_DF.to_sql(table_name, engine, if_exists='append', index=False, dtype=data_dict)
        

def update_map_data(date, concept_status:pd.DataFrame):
    """
    date: str 日期
    concept_status: DataFrame 概念状态表
    """
    stock_concept = cp.get_stock_concepts_data(concept_status, date)
    concept_stock = cp.get_concept_stocks_data(concept_status, date)
    stock_concept['record_date'] = date
    concept_stock['record_date'] = date
    stock_concept['updatetime'] = datetime.datetime.now()
    concept_stock['updatetime'] = datetime.datetime.now()
    data_dict = {'wind_code': VARCHAR(length=9), 'count': INT(), 'record_date': DATE(), 'updatetime': DATETIME()}
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
    stock_concept.to_sql('ths_stock_concepts_map', engine, if_exists='append', index=False, dtype=data_dict)
    concept_stock.to_sql('ths_concept_stocks_map', engine, if_exists='append', index=False, dtype=data_dict)

    

#################################################################################### 更新概念数据 ##################################################################


#################################################################################### 概念指数数据 ##################################################################
def update_concept_price_index(concept_status:pd.DataFrame,
                               start_date:str,
                               end_date:str):
    """
    更新概念价格指数数据
    """
    concept_similarity_status = cp.concept_similarity_status(concept_status)
    daily_return = cp.daily_retun_data(start_date, end_date)
    concepts_ls = concept_status['concept'].unique().tolist()
    concept_index_DF = pd.DataFrame(columns=concepts_ls)
    for concept in concepts_ls:
        concept_index = cp.calculate_concept_price_index(concept, concept_similarity_status, daily_return)
        concept_index_DF[concept] = concept_index
    return concept_index_DF


def update_concept_hot_index(concept_ls:list,
                             start_date:str,
                             end_date:str):
    """
    更新概念热度指数数据
    """
    concept_hot_index_DF = pd.DataFrame()
    for concept in concept_ls:
        concept_hot_index = cp.concept_hot_index(concept, date_range=f"{start_date}~{end_date}")
        concept_hot_index_DF = pd.concat([concept_hot_index_DF, concept_hot_index], axis=0, ignore_index=True)
    return concept_hot_index_DF