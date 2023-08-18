import logging
from iFinDPy import *
import datetime
import pandas as pd
import numpy as np
import calendar
from ConceptEvent import ConceptEvent
from sqlalchemy import create_engine, VARCHAR
import matplotlib.pyplot as plt

# 设置日志
logging.basicConfig(filename='concept_helper.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


# 登录函数
def thslogin():
    # 输入用户的帐号和密码
    thsLogin = THS_iFinDLogin("hacb231","fbede3")
    if thsLogin != 0:
        logging.info('登录失败')
        return False
    else:
        logging.info('登录成功')
        return True

######################################################################################################## 数据获取函数 #############################################################
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
        logging.info('概念%s数据获取成功' % name)
        return final_df
    except Exception as e:
        logging.error('概念%s数据获取失败' % name)
        logging.error(e)
        return pd.DataFrame()
        
# 股票概念映射表
def get_concept_stocks_data(concept_status: pd.DataFrame, date: datetime.date):
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
def get_stock_concepts_data(concept_status: pd.DataFrame, date: datetime.date):
    target_df = concept_status[concept_status['tradedate'] <= date]
    target_sorted = target_df.sort_values(by=['wind_code', 'concept', 'tradedate'])
    latest_status = target_sorted.drop_duplicates(subset=['wind_code', 'concept'], keep='last')
    # 过滤出纳入的记录
    in_status = latest_status[latest_status['status'] == '纳入']
    grouped = in_status.groupby('wind_code').agg(concepts = ('concept', 'unique'),
                                                 count = ('concept', 'size')).reset_index()
    grouped['concepts'] = grouped['concepts'].apply(lambda x: ','.join(map(str, x)))
    return grouped

######################################################################################################## 数据获取函数 #############################################################


# 概念热度指数获取
def concept_hot_index(concept:str, date_range:str):
    """
    计算概念综合新闻指数
    """
    id = concept_id_map()[concept]
    concept_hot_index = ConceptEvent.get_concept_ht_index(id, date_range)
    concept_hot_index['date'] = concept_hot_index['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    concept_hot_index['val2'] = concept_hot_index['val2'].replace('', np.nan)
    concept_hot_index['val2'] = concept_hot_index['val2'].astype(np.float64)
    concept_hot_index['val2'] = concept_hot_index['val2'].fillna(method='ffill')
    concept_hot_index['signal'] = concept_shift_signal(concept_hot_index['val1'], lbd=2)
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

##################################################################### 方便的工具函数 #####################################################################
# 获取月度最后一天    
def get_last_days(start_date, end_date):
    """
    获取指定日期范围内的月度最后一天
    start_date: str 开始日期
    end_date: str 结束日期
    """
    date_ls = []
    current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    while current_date <= end_date:
        last_day = calendar.monthrange(current_date.year, current_date.month)[1]
        last_day_date = datetime.date(current_date.year, current_date.month, last_day)
        date_ls.append(last_day_date.strftime('%Y-%m-%d'))
        current_date = current_date.replace(day=1) + datetime.timedelta(days=32)

    return date_ls

def ts_date(str):
    """
    将str数据转换为datetime.date数据
    """
    return datetime.datetime.strptime(str, '%Y-%m-%d').date()

def concept_id_map():
    """
    获取概念名称与id的映射
    """
    return ConceptEvent.get_concept_all()[['id', 'name']].set_index('name').to_dict()['id']

def concept_shift_signal(index_series: pd.Series, 
                         lbd: float = 2):
    """
    根据概念热度生成异动信号
    """
    Boll = index_series.rolling(window=20).mean()
    std = index_series.rolling(window=20).std()

    upper = Boll + lbd * std
    signal = (index_series.shift(1) < upper.shift(1)) & (index_series > upper)
    signal = signal.astype(int)
    
    return signal

def plot_figure(df):
    fig, ax1 = plt.subplots(figsize=(14, 7))
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Concept_Index', color='blue')
    ax1.plot(df['date'], df['concept_index'], color='blue', label='Concept Index')
    ax1.tick_params(axis='y', labelcolor='blue')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Hot Index', color='green')
    ax2.plot(df['date'], df['hot_index'], color='green', label='Hot Index')
    ax2.tick_params(axis='y', labelcolor='green')

    for _, row in df.iterrows():
        if row['signal'] == 1:
            ax1.scatter(row['date'], row['concept_index'], facecolors='none', edgecolors='red', s=100, linewidths=1.5)
    
    plt.title('Concept Index & Hot Index')
    plt.grid(True)
    plt.tight_layout()
    plt.show()