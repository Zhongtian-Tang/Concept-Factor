# 股票概念因子处理

这是一个用于处理股票概念数据的 Python 项目，它提供了获取同花顺所有概念、获取概念标的、获取概念热度数据、获取概念异动新闻等功能。

## 安装

使用包管理器 [pip](https://pip.pypa.io/en/stable/) 来安装依赖。

```bash
pip install -r requirements.txt
```

## 首先

导入必要的模块:

```python
import concept_helper
import ConceptEvent
import TokenUtil
import main
```

**为了使用 TokenUtil 类，需要提供同花顺 API 的 APPKEY 和 APPSECRET。**

```python
token_util = TokenUtil.TokenUtil()
token_util.set_APPKEY('你的_APPKEY', '你的_APPSECRET')
access_token = token_util.get_token(token_util)
```

## 代码获取模块

dependencies文件夹里concept_helper.py包含了主要的数据获取函数

```python
import concept_helper as cp
cp.get_concept_status(id, name) ## 获取指定概念的纳入记录
cp.get_concept_stockc_data(concept_status, date) ## 根据纳入调出记录整理概念 > 股票映射表(指定日期date)
cp.get_stock_concepts_data(concept_status, date) ## 整理股票 > 概念映射表(指定日期date)
```

## 指数编制模块

根据数据库中的数据，筛选出包含股票大于10的股票并以相似度为权重，编制收益率价格指数

```python
# 获取每个概念下每支股票的每日的激活态相似度，若没有数据，权重为0
concept_similarity_status = cp.concept_similarity_status(concept_status)
daily_return = cp.daily_return_data(start_date, end_date) # 规定指数回测时间范围，获取对应时间内的收益率
concept_price_index = cp.calculate_concept_price_index(concept, concept_similarity_status, daily_return) # 指定概念的价格收益指数
concept_hot_index = cp.concept_hot_index(concept, daterange)  # 获取指定时间范围内概念的热度指数与综合指数，包括异动时间
```

## 主函数脚本

src里的main.py文件包含运行函数，将生成的数据写入数据库

```python
update_concepts_status() #更新最新的概念记录表并写入数据库
update_map_data(date) #更新最新月末日期的股票 <> 概念映射表并写入数据库
```

## 工具函数

```python
import concept_helper as cp
cp.ts_date(str) #转化字符串为datetime.date数据
cp.concept_shift_signal(pd.Series) # 根据概念热度指数生成异动信号
cp.plot_figure(pd.DataFrame) # 异动信号可视化
```
