from loguru import logger
import datetime
import pandas as pd
import numpy as np
import calendar
from concept_factor.dependencies.ConceptEvent import ConceptEvent
from concept_factor.src.logger_config import setup_logger
from sqlalchemy import create_engine, VARCHAR
import matplotlib.pyplot as plt

setup_logger()

############################################################### 数据获取函数 #############################################################
# 获取概念纳入数据
def get_concept_status(id, name):
    """
    id: str 概念id
    name: str 概念名称
    concept_df: DataFrame 概念纳入数据
    """
    try:
        symbols = ConceptEvent.get_concept_symbol(id)['symbols']
        final_df = (pd.DataFrame(symbols).assign(
            wind_code=lambda df: df['code'].astype(str),
            sec_name=lambda df: df['name'].astype(str),
            similarity=lambda df: pd.to_numeric(df['conceptSimilarity'], errors='coerce').fillna(0).astype(float),
            status=lambda df: df['isvalid'].replace({1: '纳入', 0: '剔除'}).astype(str),
            tradedate=lambda df: df['addDate'].fillna(df['delDate']).astype('datetime64[D]'),
            concept=name,
            updatetime=datetime.datetime.now()
        )
        [['wind_code', 'sec_name', 'tradedate', 'similarity','concept','status','updatetime']]
        )
        return final_df
    except Exception as e:
        logger.error(f"概念{name}数据获取失败: {e}")
        return pd.DataFrame()
        
# 股票概念映射表
def get_concept_stocks_data(concept_status: pd.DataFrame, date: str):
    concept_status['tradedate'] = pd.to_datetime(concept_status['tradedate'])
    target_df = concept_status[concept_status['tradedate'] <= date]
    def stocks_in_concept_on_date(sub_df, concept_name):
        concept_df = sub_df[sub_df['concept'] == concept_name]
        in_stocks = concept_df[concept_df['status'] == '纳入']['wind_code'].unique()
        out_stocks = concept_df[concept_df['status'] == '剔除']['wind_code'].unique()
        stocks_on_date = np.setdiff1d(in_stocks, out_stocks)
        return stocks_on_date
    result = {}
    for concept_name in target_df['concept'].unique():
        result[concept_name] = stocks_in_concept_on_date(target_df, concept_name)
    result_df = pd.DataFrame({
        'concept': result.keys(),
        'stocks': [list(stocks) for stocks in result.values()],
        'count': [len(stocks) for stocks in result.values()]})
    result_df['stocks'] = result_df['stocks'].apply(lambda x: ','.join(map(str, x)))
    return result_df

# 概念股票映射表
def get_stock_concepts_data(concept_status: pd.DataFrame, date: str):
    concept_status['tradedate'] = pd.to_datetime(concept_status['tradedate'])
    target_df = concept_status[concept_status['tradedate'] <= date]
    target_sorted = target_df.sort_values(by=['wind_code', 'concept', 'tradedate'])
    latest_status = target_sorted.drop_duplicates(subset=['wind_code', 'concept'], keep='last')
    # 过滤出纳入的记录
    in_status = latest_status[latest_status['status'] == '纳入']
    grouped = in_status.groupby('wind_code').agg(concepts = ('concept', 'unique'),
                                                 count = ('concept', 'size')).reset_index()
    grouped['concepts'] = grouped['concepts'].apply(lambda x: ','.join(map(str, x)))
    return grouped

############################################################## 数据获取函数 #############################################################

############################################################## 数据库操作函数 ############################################################
# 概念热度指数获取
def concept_hot_index(concept:str, date_range:str):
    """
    计算概念综合新闻指数
    date_range例子: '2014-01-01~2023-08-31'
    """
    id = concept_id_map()[concept]
    concept_hot_index = ConceptEvent.get_concept_ht_index(id, date_range)
    concept_hot_index['date'] = concept_hot_index['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    concept_hot_index['val2'] = concept_hot_index['val2'].replace('', np.nan)
    concept_hot_index['val2'] = concept_hot_index['val2'].astype(np.float64)
    concept_hot_index['val2'] = concept_hot_index['val2'].fillna(method='ffill')
    concept_hot_index['signal'] = concept_shift_signal(concept_hot_index['val1'], lbd=3)
    concept_hot_index.rename(columns={'date':'tradedate','val2': 'concept_index', 'val1': 'hot_index'}, inplace=True)
    concept_hot_index['concept'] = concept

    return concept_hot_index[['tradedate','concept', 'concept_index', 'hot_index', 'signal']]

# 概念标的状态表
def concept_similarity_status(concept_status: pd.DataFrame):
    """
    计算当期概念标的的相似度激活状态
    """
    concept_status_sorted = concept_status.sort_values(by=['tradedate'])
    concept_status_sorted['status_val'] = concept_status_sorted['status'].replace({'纳入': 1, '剔除': -1})
    cum_status = pd.pivot_table(concept_status_sorted, values='status_val', index='tradedate', columns=['concept', 'wind_code'], aggfunc='sum').cumsum().fillna(method='ffill')
    active_stocks = (cum_status > 0).astype(int)
    simpilarity_pivot = pd.pivot_table(concept_status_sorted, index='tradedate', columns=['concept', 'wind_code'], values='similarity').fillna(method='ffill')
    similarity_status_data = active_stocks * simpilarity_pivot
    return similarity_status_data

def daily_return_data(start_date: str, end_date:str):
    """
    选取指定范围内的股票日收益率数据
    这里的范围与热度指数的范围保持一致
    """
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/jydb?charset=utf8')
    query = f"""
    SELECT DATETIME, wind_code, PCT_CHG FROM jydb.dstock
    WHERE DATETIME BETWEEN '{start_date}' AND '{end_date}'
    """
    return_df = pd.read_sql(query, engine)
    daily_return_pivot = pd.pivot_table(return_df, values='PCT_CHG', index='DATETIME', columns='wind_code')
    return daily_return_pivot

def calculate_concept_price_index(concept: str,
                                  concept_similarity_status: pd.DataFrame,
                                  daily_return_pivot: pd.DataFrame):
    """
    计算加权概念价格指数
    """
    concept_similarity_status = concept_similarity_status.reindex(daily_return_pivot.index).fillna(method='ffill')
    target_df = concept_similarity_status.loc[:, concept]
    weights = target_df.apply(lambda x: x / x.sum(), axis=1)
    weighted_return = (daily_return_pivot * weights).sum(axis=1)
    return weighted_return

##################################################################### 数据库操作函数 #####################################################################
