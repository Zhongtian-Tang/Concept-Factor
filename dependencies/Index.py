from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np


def get_data(name):
    engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
    df = pd.read_sql(name, engine)
    return df


concept_stock = get_data('ths_concept_stock_map')
stock_concept = get_data('ths_stock_concept_map')
concept_status =get_data('ths_concept_status')
concept_status_no0 = concept_status[concept_status['similarity'] != 0]                      # 保留similarity=0的概念，方便后续操作
# concept_stock = concept_stock[(concept_stock['tradedate'] == '2023-07-25') & (concept_stock['stock_num'] >= 10)]     最好根据选取时间来获取最新概念
# stock_concept = stock_concept[stock_concept['tradedate'] == '2023-07-25']

concepts_ls_1 = concept_stock[concept_stock['stock_num'] >= 10]['concepts'].unique()
concepts_ls_2 = concept_status_no0['concept'].value_counts()[(concept_status_no0['concept'].value_counts() >= 10).values].index
common_concepts = np.intersect1d(concepts_ls_1, concepts_ls_2)


def check_difference(concepts):
    """
    检查两种不同概念构造方法的差异
    """
    DF = {}
    i = 0
    for concept in concepts:
        a = concept_stock[concept_stock['concepts'] == concept]['stock_codes'].values[0].split(',')
        b = concept_status_no0[concept_status_no0['concept'] == concept]['wind_code'].unique()
        diff = np.setdiff1d(a, b)
        if diff.size > 0 and np.intersect1d(a, b).size > 0:
            DF[concept] = diff
        else:
            i += 1
    print("check number: ", i)
    return DF



