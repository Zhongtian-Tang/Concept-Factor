'''
Author: Tangzhong-Tian 116010205@link.cuhk.edu.cn
Date: 2023-08-03 12:48:36
LastEditors: Tangzhong-Tian 116010205@link.cuhk.edu.cn
LastEditTime: 2023-08-03 13:35:31
FilePath: \Concept-Factor\dependencies\ConceptHot.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from ConceptEvent import ConceptEvent
import pandas as pd
import numpy as np


def concept_shift_signal(index_series: pd.Series, 
                         lbd: float = 2):
    Boll = index_series.rolling(window=20).mean()
    std = index_series.rolling(window=20).std()

    upper = Boll + lbd * std
    signal = (index_series.shift(1) < upper.shift(1)) & (index_series > upper)
    signal = signal.astype(int)
    
    return signal