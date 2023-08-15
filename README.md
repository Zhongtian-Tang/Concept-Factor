# 股票概念因子处理

这是一个用于处理股票概念因子的 Python 项目，它提供了获取同花顺所有概念、获取概念标的、获取概念热度数据、获取概念异动新闻等功能。

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

**为了使用 TokenUtil 类，你需要提供同花顺 API 的 APPKEY 和 APPSECRET。**

```python
token_util = TokenUtil.TokenUtil()
token_util.set_APPKEY('你的_APPKEY', '你的_APPSECRET')
access_token = token_util.get_token(token_util)
```

**运行主函数，使用以下命令**

```python
main.job('2023-07-25')  # 用你需要的日期替换 '2023-07-25'
```
