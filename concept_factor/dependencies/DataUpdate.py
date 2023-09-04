from loguru import logger
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DATE, FLOAT, INT, DATETIME
import datetime
import concept_factor.dependencies.concept_helper as cp
from concept_factor.dependencies.ConceptEvent import ConceptEvent
from concept_factor.src.logger_config import setup_logger

setup_logger()

def update_concepts_status():
    """
    更新市场概念纳入剔除表
    """
    try:
        concepts_df = ConceptEvent().get_concept_all()[['id', 'name']]
        concepts_DF = pd.DataFrame()
        for _, concept in concepts_df.iterrows():
            concept_id = concept['id']
            concept_name = concept['name']
            concept_status = cp.get_concept_status(concept_id, concept_name)
            concepts_DF = pd.concat([concepts_DF, concept_status], axis=0, ignore_index=True)
        concepts_DF = concepts_DF.sort_values(by=['concept', 'tradedate'])
        engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
        table_name = 'ths_concepts_status'
        data_dict = {'wind_code': VARCHAR(length=9), 'sec_name':VARCHAR(30),
                'valid': INT(), 'tradedate': DATE(), 'similarity': FLOAT(),
                'concept': VARCHAR(30),'status':VARCHAR(50), 'updatetime': DATETIME()}
        concepts_DF.to_sql(table_name, engine, if_exists='append', index=False, dtype=data_dict)
        return concepts_DF
    except Exception as e:
        logger.error(f"更新市场概念纳入剔除表失败: {e}")
        return pd.DataFrame()

def update_map_data(date, concept_status:pd.DataFrame):
    """
    date: str 日期
    concept_status: DataFrame 概念状态表
    """
    try:
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
    except Exception as e:
        logger.error(f"更新概念股票映射表失败: {e}")

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