"""
レースデータの基本分析を行うスクリプト (2020-2024)
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pathlib import Path

# srcディレクトリをパスに追加
sys.path.append(str(Path(__file__).parent.parent))

from src.data.database import execute_query, query_with_cache

# 出力ディレクトリの設定
OUTPUT_DIR = Path(__file__).parent.parent / "output" / "analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def analyze_race_data():
    """2020-2024年のレースデータを分析"""
    print("レースデータ基本分析開始...")
    
    # 分析1: 年度別レース数
    yearly_race_count_query = """
    SELECT 
        kaisai_nen as year, 
        COUNT(*) as race_count
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY kaisai_nen
    ORDER BY kaisai_nen
    """
    yearly_race_count = query_with_cache(yearly_race_count_query, "yearly_race_count_2020_2024")
    print("\n年度別レース数:")
    print(yearly_race_count)
    
    # プロット保存
    plt.figure(figsize=(10, 6))
    sns.barplot(x='year', y='race_count', data=yearly_race_count)
    plt.title('年度別レース数 (2020-2024)')
    plt.xlabel('年')
    plt.ylabel('レース数')
    plt.savefig(OUTPUT_DIR / "yearly_race_count.png")
    plt.close()
    
    # 分析2: 競馬場別レース数
    course_race_count_query = """
    SELECT 
        keibajo_code,
        CASE 
            WHEN keibajo_code = '01' THEN '札幌'
            WHEN keibajo_code = '02' THEN '函館'
            WHEN keibajo_code = '03' THEN '福島'
            WHEN keibajo_code = '04' THEN '新潟'
            WHEN keibajo_code = '05' THEN '東京'
            WHEN keibajo_code = '06' THEN '中山'
            WHEN keibajo_code = '07' THEN '中京'
            WHEN keibajo_code = '08' THEN '京都'
            WHEN keibajo_code = '09' THEN '阪神'
            WHEN keibajo_code = '10' THEN '小倉'
            ELSE keibajo_code
        END AS course_name,
        COUNT(*) as race_count
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY keibajo_code
    ORDER BY keibajo_code
    """
    course_race_count = query_with_cache(course_race_count_query, "course_race_count_2020_2024")
    print("\n競馬場別レース数:")
    print(course_race_count)
    
    # プロット保存
    plt.figure(figsize=(12, 6))
    sns.barplot(x='course_name', y='race_count', data=course_race_count)
    plt.title('競馬場別レース数 (2020-2024)')
    plt.xlabel('競馬場')
    plt.ylabel('レース数')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "course_race_count.png")
    plt.close()
    
    # 分析3: トラック種別・馬場状態
    track_condition_query = """
    SELECT 
        SUBSTRING(track_code, 1, 1) as track_type,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN '芝'
            WHEN SUBSTRING(track_code, 1, 1) = '2' THEN 'ダート'
            ELSE 'その他'
        END AS track_name,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN babajotai_code_shiba
            ELSE babajotai_code_dirt
        END AS track_condition,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN
                CASE 
                    WHEN babajotai_code_shiba = '1' THEN '良'
                    WHEN babajotai_code_shiba = '2' THEN '稍重'
                    WHEN babajotai_code_shiba = '3' THEN '重'
                    WHEN babajotai_code_shiba = '4' THEN '不良'
                    ELSE babajotai_code_shiba
                END
            ELSE
                CASE 
                    WHEN babajotai_code_dirt = '1' THEN '良'
                    WHEN babajotai_code_dirt = '2' THEN '稍重'
                    WHEN babajotai_code_dirt = '3' THEN '重'
                    WHEN babajotai_code_dirt = '4' THEN '不良'
                    ELSE babajotai_code_dirt
                END
        END AS condition_name,
        COUNT(*) as race_count,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM jvd_ra 
            WHERE kaisai_nen BETWEEN '2020' AND '2024'
        ), 2) as percentage
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY track_type, track_name, track_condition, condition_name
    ORDER BY track_type, track_condition
    """
    track_condition = query_with_cache(track_condition_query, "track_condition_2020_2024")
    print("\nトラック種別・馬場状態別レース数:")
    print(track_condition)
    
    # プロット保存（トラック種別×馬場状態のヒートマップ）
    track_condition_pivot = pd.pivot_table(
        track_condition, 
        values='race_count', 
        index='track_name', 
        columns='condition_name', 
        fill_value=0
    )
    
    plt.figure(figsize=(10, 6))
    sns.heatmap(track_condition_pivot, annot=True, fmt='g', cmap='Blues')
    plt.title('トラック種別×馬場状態別レース数 (2020-2024)')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "track_condition_heatmap.png")
    plt.close()
    
    # 分析4: 距離帯別レース数
    distance_query = """
    SELECT 
        CASE 
            WHEN CAST(kyori AS INTEGER) <= 1400 THEN '短距離'
            WHEN CAST(kyori AS INTEGER) <= 2000 THEN '中距離'
            ELSE '長距離'
        END AS distance_category,
        SUBSTRING(track_code, 1, 1) as track_type,
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN '芝'
            WHEN SUBSTRING(track_code, 1, 1) = '2' THEN 'ダート'
            ELSE 'その他'
        END AS track_name,
        COUNT(*) as race_count
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY distance_category, track_type, track_name
    ORDER BY distance_category, track_type
    """
    distance_data = query_with_cache(distance_query, "distance_data_2020_2024")
    print("\n距離帯別レース数:")
    print(distance_data)
    
    # プロット保存
    plt.figure(figsize=(10, 6))
    distance_pivot = pd.pivot_table(
        distance_data, 
        values='race_count', 
        index='distance_category', 
        columns='track_name', 
        fill_value=0
    )
    distance_pivot.plot(kind='bar', stacked=True)
    plt.title('距離帯・トラック種別レース数 (2020-2024)')
    plt.xlabel('距離カテゴリ')
    plt.ylabel('レース数')
    plt.legend(title='トラック種別')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "distance_track_race_count.png")
    plt.close()
    
    # 分析5: 天候別レース数
    weather_query = """
    SELECT 
        tenko_code,
        CASE 
            WHEN tenko_code = '1' THEN '晴'
            WHEN tenko_code = '2' THEN '曇'
            WHEN tenko_code = '3' THEN '小雨'
            WHEN tenko_code = '4' THEN '雨'
            WHEN tenko_code = '5' THEN '小雪'
            WHEN tenko_code = '6' THEN '雪'
            ELSE tenko_code
        END AS weather_name,
        COUNT(*) as race_count,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM jvd_ra 
            WHERE kaisai_nen BETWEEN '2020' AND '2024'
        ), 2) as percentage
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY tenko_code, weather_name
    ORDER BY race_count DESC
    """
    weather_data = query_with_cache(weather_query, "weather_data_2020_2024")
    print("\n天候別レース数:")
    print(weather_data)
    
    # プロット保存
    plt.figure(figsize=(10, 6))
    sns.barplot(x='weather_name', y='race_count', data=weather_data)
    plt.title('天候別レース数 (2020-2024)')
    plt.xlabel('天候')
    plt.ylabel('レース数')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "weather_race_count.png")
    plt.close()
    
    # 分析6: 月別レース数
    monthly_query = """
    SELECT 
        SUBSTRING(kaisai_tsukihi, 1, 2) as month,
        COUNT(*) as race_count
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY month
    ORDER BY month
    """
    monthly_data = query_with_cache(monthly_query, "monthly_data_2020_2024")
    print("\n月別レース数:")
    print(monthly_data)
    
    # プロット保存
    plt.figure(figsize=(10, 6))
    sns.barplot(x='month', y='race_count', data=monthly_data)
    plt.title('月別レース数 (2020-2024)')
    plt.xlabel('月')
    plt.ylabel('レース数')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "monthly_race_count.png")
    plt.close()
    
    # 分析7: 出走頭数分布
    runners_query = """
    SELECT 
        shusso_tosu as runners_count,
        COUNT(*) as race_count,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM jvd_ra 
            WHERE kaisai_nen BETWEEN '2020' AND '2024'
        ), 2) as percentage
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN '2020' AND '2024'
    GROUP BY shusso_tosu
    ORDER BY shusso_tosu
    """
    runners_data = query_with_cache(runners_query, "runners_data_2020_2024")
    print("\n出走頭数分布:")
    print(runners_data)
    
    # プロット保存
    plt.figure(figsize=(10, 6))
    sns.barplot(x='runners_count', y='race_count', data=runners_data)
    plt.title('出走頭数分布 (2020-2024)')
    plt.xlabel('出走頭数')
    plt.ylabel('レース数')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "runners_count_distribution.png")
    plt.close()

if __name__ == "__main__":
    analyze_race_data()
