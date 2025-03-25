"""
特徴量エンジニアリングの基底クラスとユーティリティ
"""
import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod

class BaseFeatureExtractor(ABC):
    """特徴量エンジニアリングの基底クラス"""
    
    def __init__(self, cache_dir=None):
        """
        初期化
        
        Args:
            cache_dir: キャッシュディレクトリのパス (default: '../../data/cache')
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(__file__), '../../data/cache')
        
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    @abstractmethod
    def extract(self, race_data, horse_data):
        """
        特徴量を抽出するメソッド
        
        Args:
            race_data: レース情報
            horse_data: 馬情報
            
        Returns:
            DataFrame: 抽出した特徴量
        """
        pass
    
    def _cache_path(self, name):
        """キャッシュファイルのパスを取得"""
        return os.path.join(self.cache_dir, f"{name}.pkl")
    
    def load_from_cache(self, name):
        """キャッシュからデータを読み込む"""
        cache_path = self._cache_path(name)
        if os.path.exists(cache_path):
            return pd.read_pickle(cache_path)
        return None
    
    def save_to_cache(self, data, name):
        """データをキャッシュに保存"""
        cache_path = self._cache_path(name)
        data.to_pickle(cache_path)

# ユーティリティ関数

def create_race_id(row):
    """レースIDを生成する
    
    Format: {year}{month_day}{course_code}{race_number}
    Ex: 20210101050112 = 2021年1月1日 東京12レース
    """
    return f"{row['kaisai_nen']}{row['kaisai_tsukihi']}{row['keibajo_code']}{row['race_bango']}"

def create_horse_race_id(row):
    """馬×レースの組み合わせIDを生成する
    
    Format: {race_id}_{horse_id}
    """
    race_id = create_race_id(row)
    return f"{race_id}_{row['ketto_toroku_bango']}"

def encode_categorical(df, column, prefix=None):
    """カテゴリ変数をダミー変数に変換
    
    Args:
        df: DataFrame
        column: 変換対象カラム名
        prefix: 変換後のカラム名プレフィックス (default: column名)
        
    Returns:
        DataFrame: ダミー変数に変換されたDataFrame
    """
    prefix = prefix or column
    dummies = pd.get_dummies(df[column], prefix=prefix)
    return pd.concat([df, dummies], axis=1)

def normalize_numeric(df, column, method='min_max'):
    """数値列を正規化
    
    Args:
        df: DataFrame
        column: 正規化するカラム名
        method: 正規化手法 ('min_max'または'standard')
        
    Returns:
        DataFrame: 数値が正規化されたDataFrame
    """
    if method == 'min_max':
        df[f"{column}_norm"] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
    elif method == 'standard':
        df[f"{column}_norm"] = (df[column] - df[column].mean()) / df[column].std()
    return df

def bin_numeric(df, column, bins, labels=None):
    """数値カラムをビン（範囲）に分割
    
    Args:
        df: DataFrame
        column: 分割するカラム名
        bins: ビン（範囲）の区切り値リスト
        labels: 各ビンのラベル (default: None = 0, 1, 2, ...)
        
    Returns:
        DataFrame: ビン分割された列が追加されたDataFrame
    """
    df[f"{column}_bin"] = pd.cut(df[column], bins=bins, labels=labels)
    return df

def calculate_win_rate(group):
    """グループの勝率を計算
    
    Args:
        group: Pandas GroupBy結果
        
    Returns:
        float: 勝率（パーセント）
    """
    if len(group) == 0:
        return 0.0
    wins = (group['kakutei_chakujun'] == '01').sum()
    return (wins / len(group)) * 100

def calculate_roi(group):
    """グループの回収率を計算
    
    Args:
        group: Pandas GroupBy結果
        
    Returns:
        float: 回収率（パーセント）
    """
    if len(group) == 0:
        return 0.0
    
    total_races = len(group)
    total_return = 0.0
    
    for _, row in group.iterrows():
        if row['kakutei_chakujun'] == '01':  # 1着
            total_return += float(row['tansho_odds']) / 10.0  # オッズを勝ち金に変換
    
    return (total_return / total_races) * 100
