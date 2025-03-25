"""
データベース構造を分析するスクリプト
"""
import os
import pandas as pd
import sys
from pathlib import Path

# srcディレクトリをパスに追加
sys.path.append(str(Path(__file__).parent.parent))

from src.data.database import list_tables, get_table_schema, execute_query

def analyze_database_structure():
    """データベース構造を分析して出力"""
    print("データベース構造分析開始...")
    
    # テーブル一覧を取得
    tables = list_tables()
    print(f"データベースに存在するテーブル ({len(tables)}):")
    for table in tables:
        print(f"  - {table}")
    
    # 各テーブルの構造を分析
    for table in tables:
        print(f"\n{table} テーブルの構造:")
        columns = get_table_schema(table)
        
        # カラム情報を表示
        print(f"カラム数: {len(columns)}")
        for column in columns:
            print(f"  - {column['name']}: {column['type']}")
        
        # レコード数を取得
        count_query = f"SELECT COUNT(*) as count FROM {table}"
        count_result = execute_query(count_query)
        record_count = count_result['count'].iloc[0]
        print(f"レコード数: {record_count}")
        
        # サンプルデータを表示（最初の5行）
        if record_count > 0:
            sample_query = f"SELECT * FROM {table} LIMIT 5"
            sample_data = execute_query(sample_query)
            print("\nサンプルデータ:")
            print(sample_data)
        
        # 主要テーブルの場合、追加情報を表示
        if table in ['jvd_ra', 'jvd_se', 'jvd_hr', 'jvd_um', 'jvd_hn']:
            if table == 'jvd_ra':
                # レース開催年の範囲を表示
                year_query = """
                SELECT 
                    MIN(kaisai_nen) as min_year, 
                    MAX(kaisai_nen) as max_year,
                    COUNT(DISTINCT kaisai_nen) as year_count
                FROM jvd_ra
                """
                year_data = execute_query(year_query)
                print("\nレース開催年の範囲:")
                print(year_data)
            
            elif table == 'jvd_se':
                # 着順の分布を表示
                finish_query = """
                SELECT 
                    kakutei_chakujun, 
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM jvd_se), 2) as percentage
                FROM jvd_se
                GROUP BY kakutei_chakujun
                ORDER BY kakutei_chakujun
                LIMIT 20
                """
                finish_data = execute_query(finish_query)
                print("\n着順の分布 (上位20):")
                print(finish_data)

if __name__ == "__main__":
    analyze_database_structure()
