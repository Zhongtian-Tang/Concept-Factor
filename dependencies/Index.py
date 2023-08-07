from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np
import logging

def load_data(date: str):
    """
    从数据库中获取数据,筛选掉similarity异常与样本太少的概念, 返还数据格式为DataFrame
    """
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
    concept_status = pd.DataFrame(engine.execute('SELECT * FROM ths_concept_status WHERE similarity > 0').fetchall())
    concept_status.drop(['updatetime'], axis=1, inplace=True)
    cond = concept_status['concept'].value_counts()[(concept_status['concept'].value_counts() >= 10).values].index
    concept_status.set_index('concept', inplace=True)
    concept_status = concept_status.loc[cond]
    concept_status.reset_index(inplace=True, names='concept')
    query = "SELECT * FROM ths_concept_stock_map WHERE tradedate = %s AND stock_num > 10"
    concept_stock = pd.read_sql_query(query, engine, params=[date])
    concept_stock.drop(['updatetime'], axis=1, inplace=True)
    concept_stock['stock_codes'] = concept_stock['stock_codes'].apply(lambda x: x.split(','))
    return concept_status, concept_stock


def get_concept_stock_dict(concept_status: pd.DataFrame,
                           concept_stock: pd.DataFrame):
    """
    筛选出数据清理后的概念与所属标的池, 返还一个字典
    """
    common_concepts = np.intersect1d(concept_status['concept'].unique(), concept_stock['concepts'].unique())
    concept_stock_dict = {}
    for concept in common_concepts:
        codes_A = concept_status[concept_status['concept'] == concept]['wind_code']
        codes_B = concept_stock[concept_stock['concepts'] == concept]['stock_codes'].values[0]
        common_codes = np.intersect1d(codes_A, codes_B)
        if common_codes.size == 0:
            logging.info('概念 {} 异常'.format(concept))
    else:
        concept_stock_dict[concept] = common_codes
    logging.info('概念与标的池筛选完成, 今日概念数量: {}'.format(len(concept_stock_dict.keys())))
    return concept_stock_dict



