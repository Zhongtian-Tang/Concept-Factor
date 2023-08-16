import logging
from iFinDPy import *
import datetime
import pandas as pd
import numpy as np
import calendar
from ConceptEvent import ConceptEvent
from sqlalchemy import create_engine, VARCHAR

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
def concept_stocks_data(concept_status: pd.DataFrame, date: datetime.date):
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
def stock_concepts_data(concept_status: pd.DataFrame, date: datetime.date):
    target_df = concept_status[concept_status['tradedate'] <= date]
    target_sorted = target_df.sort_values(by=['wind_code', 'concept', 'tradedate'])
    latest_status = target_sorted.drop_duplicates(subset=['wind_code', 'concept'], keep='last')
    # 过滤出纳入的记录
    in_status = latest_status[latest_status['status'] == '纳入']
    grouped = in_status.groupby('wind_code').agg(concepts = ('concept', 'unique'),
                                                 count = ('concept', 'size')).reset_index()
    grouped['concepts'] = grouped['concepts'].apply(lambda x: ','.join(map(str, x)))
    return grouped

# 概念热度指数获取
def conbcept_hot_index():
    """
    统计当期标的数大于10的指数并计算热度指数
    """





# 概念价格指数构建
def concept_price_index(concept: str, date: datetime.date, 
                        return_df: pd.DataFrame, 
                        concept_status: pd.DataFrame,
                        concept_stocks: pd.DataFrame):
    """
    计算当期概念主题的收益率
    """
    # 相似度权重
    target_df = concept_status[(concept_status['tradedate'] <= date) & (concept_status['concept'] == concept)]
    in_df = target_df[target_df['status'] == '纳入']['wind_code']
    out_df = target_df[target_df['status'] == '剔除']['wind_code']
    codes_list = np.setdiff1d(in_df, out_df)
    similarity = target_df[target_df['wind_code'].isin(codes_list)]['similarity']
    # 获取股票价格数据
    stock_return = return_df[(return_df['wind_code'].isin(codes_list)) & (return_df['DATETIME'] == date)]['PCT_CHG']
    return np.dot(stock_return, similarity)
