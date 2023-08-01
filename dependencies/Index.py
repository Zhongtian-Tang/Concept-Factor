from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np


def get_data(name):
    engine = create_engine('mysql+pymysql://root:123456@10.224.1.70:3306/stock?charset=utf8')
    df = pd.read_sql(name, engine)
    return df


concept_stock = get_data('ths_concept_stock_map')
stock_concept = get_data('ths_stock_concept_map')
concept_status =get_data('ths_concept_status')
concept_status['similarity'] = pd.to_numeric(concept_status['similarity'])
concept_status_no0 = concept_status[concept_status['similarity'] != 0]

concepts_ls_1 = concept_stock[concept_stock['stock_num'] >= 10]['concept'].to_list()
concept_sta = concept_status.groupby('concept').count()
concepts_ls_2 = concept_sta[concept_sta['wind_code'] >= 10].index.to_list()

final_concepts_ls = list(set(concepts_ls_1) & set(concepts_ls_2))               # 获得比较全的概念列表


