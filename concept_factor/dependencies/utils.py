from concept_factor.dependencies.ConceptEvent import ConceptEvent
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def concept_id_map():
    """
    获取概念名称与id的映射
    """
    return ConceptEvent.get_concept_all()[['id', 'name']].set_index('name').to_dict()['id']

def concept_shift_signal(index_series: pd.Series, 
                         lbd: float = 3):
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