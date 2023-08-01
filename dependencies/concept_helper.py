import logging
from iFinDPy import *
import datetime
import pandas as pd
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
    def calculate_status_and_recordDate(row):
        if pd.notna(row['addDate']):
            return '纳入', row['addDate']
        elif pd.notna(row['delDate']):
            return '剔除', row['delDate']
        else:
            return '未知', None
    try:
        symbols = ConceptEvent.get_concept_symbol(id)['symbols']
        df = pd.DataFrame(symbols)
        df = df[['code', 'delDate', 'conceptSimilarity', 'name', 'addDate']]
        df['wind_code'] = df['code'].astype(str).str.slice(0, 9)
        df['sec_name'] = df['name'].astype(str).str.slice(0, 30)
        df['conceptSimilarity'] = pd.to_numeric(df['conceptSimilarity'])
        df['conceptSimilarity'] = df['conceptSimilarity'].fillna(0)
        df['similarity'] = df['conceptSimilarity'].astype(float)
        df[['status', 'tradedate']] = df.apply(calculate_status_and_recordDate, axis=1, result_type='expand')
        df['tradedate'] = df['tradedate'].astype(datetime.date)
        df['status'] = df['status'].astype(str).str.slice(0, 30) 
        df['concept'] = name
        df['concept'] = df['concept'].astype(str).str.slice(0, 30)
        df['updatetime'] = datetime.datetime.now()
        final_df = df[['wind_code', 'sec_name', 'tradedate', 'similarity','concept','status']]
        logging.info('概念%s数据获取成功' % name)
        return final_df
    except Exception as e:
        logging.error('概念%s数据获取失败' % name)
        logging.error(e)
        return pd.DataFrame()
    

# 概念纳入表
def concept_data(update_date, start_date, end_date, index_codes):
    """
    通过数据池获取所有概念指数的纳入剔除记录, 返回概念纳入表
    
    update_date: datetime 更新日期
    start_date: str 开始日期
    end_date: str 结束日期
    index_codes: DataFrame 概念指数代码列表
    final_df: DataFrame 概念纳入表
    """
    append_data = []
    for _, index_code in index_codes.iterrows():
        concept_record = THS_DR('p03316','iv_zsdm={};iv_sdate={};iv_edate={};iv_zt=全部'.format(index_code['codes'], start_date, end_date)
                                ,'p03316_f001:Y,p03316_f002:Y,p03316_f003:Y,p03316_f004:Y','format:dataframe')              # 获取指定日期范围内概念指数纳入剔除纪录
        if concept_record.errorcode != 0:
            logging.info('缺少 {} 指数的进出纪录'.format(index_code['index_name']))
            continue
        else:
            df = concept_record.data   
            df['INDEX_CODE'] = index_code['codes']
            df['INDEX_NAME'] = index_code['index_name']
            append_data.append(df)                                                                                           # 设置日期为索引
    final_df = pd.concat(append_data,ignore_index=True)
    final_df = final_df.rename(columns={'p03316_f001':'RECORD_DATE','p03316_f002':'wind_code','p03316_f003':'SEC_NAME','p03316_f004':'STATUS'}) # 重命名列名
    final_df.astype({'RECORD_DATE': 'str', 'wind_code': 'str','SEC_NAME':'str','STATUS':'str'})        # 将列的数据类型转换为字符串
    final_df['RECORD_DATE'] = pd.to_datetime(final_df['RECORD_DATE'], format='%Y%m%d')
    final_df['RECORD_DATE'] = final_df['RECORD_DATE'].dt.strftime('%Y-%m-%d')                        # 更改纳入日期格式
    final_df['updatetime'] = update_date.strftime('%Y-%m-%d')
    final_df = final_df[['updatetime','wind_code', 'SEC_NAME', 'RECORD_DATE', 'STATUS', 'INDEX_CODE', 'INDEX_NAME']]                                                             # 设置更新时间
    return final_df


# 股票概念映射表
def stock_concept_data(date='2023-07-13'):
    """
    通过数据池获取指定日期的所有A股代码, 再通过代码列表返回股票概念映射表
    
    code_ls: list 代码列表
    date: str 日期
    data_df: DataFrame 股票概念映射表
    """
    target_stock = THS_DP('block', '{};001005010'.format(date), 'date:N,thscode:Y,security_name:N')                 #001005010: A股
    if target_stock.errorcode != 0:
        logging.info('error:{}'.format(target_stock.errmsg))
        return None
    else:
        code_ls = sorted(target_stock.data['THSCODE'].tolist())
        data_result = THS_BD(code_ls, 'ths_stock_short_name_stock;ths_the_concept_stock',';{}'.format(date))
        if data_result.errorcode != 0:
            logging.info('error:{}'.format(data_result.errmsg))
            return None
        else:
            stock_concept_df = data_result.data
            stock_concept_df['concept_num'] = stock_concept_df['ths_the_concept_stock'].apply(lambda x: 0 if pd.isna(x) or x == '' else len(x.split(',')))          # 计算每只股票的概念数量, 排除掉空值
            stock_concept_df['date'] = pd.to_datetime(date)
            # stock_concept_df.set_index('date', inplace=True)
            logging.info('日期: {} 股票概念映射表生成成功'.format(date))                                                                                                      # 设置日期为索引 
    return stock_concept_df


# 概念股票映射表
def concept_stock_data(df, date='2023-07-13'):
    """
    通过股票概念映射表返回概念股票映射表
    
    df: DataFrame 股票概念映射表
    concept_sec_df: DataFrame 概念股票映射表
    """
    all_concepts = []
    for concepts in df['concepts']:
        if pd.isna(concepts) or concepts == '':       # 检查是否为空值
            continue
        all_concepts.extend(concepts.split(','))
    unique_concepts = list(set(all_concepts))            # 去重
    
    concept_stock_list = []
    for concept in unique_concepts:
        stocks_with_concept =  df[df['concepts'].str.contains(concept, regex=False)]
        concept_stock_list.append({'concept': concept, 'stock_num': len(stocks_with_concept), 'stock_code': ','.join(stocks_with_concept['wind_code'].values)})

    concept_stock_df = pd.DataFrame(concept_stock_list)
    concept_stock_df['date'] = pd.to_datetime(date)
    concept_stock_df = concept_stock_df.sort_values(by='stock_num', ascending=False)
    # concept_stock_df.set_index('date', inplace=True)          # 设置日期为索引
    logging.info('日期: {} 概念股票映射表生成成功'.format(date)) 
    return concept_stock_df


########################################################################################## 数据获取函数结束 ##########################################################################################

########################################################################################## 数据写入函数开始 ##########################################################################################
def write_data_to_sql(data_df, table_name):
    """
    将数据写入数据库
    
    data_df: DataFrame 数据
    table_name: str 表名
    """
    try:
        engine = create_engine('mysql+pymysql://tangzt:zxcv1234@10.224.1.70:3306/tangzt?charset=utf8')
        data_dict = {col: VARCHAR(length=30) for col in data_df.columns}            # 注意长度
        data_df.to_sql(table_name, engine, if_exists='replace', index=False, dytape=data_dict)
        logging.info('数据写入数据库成功')
    except Exception as e:
        logging.error('数据写入数据库失败')
        logging.error(e)



###################################################################################### 指数编制 ###########################################################################################
