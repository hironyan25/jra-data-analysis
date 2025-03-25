"""
PostgreSQLデータベースへの接続を管理するモジュール
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# データベース接続情報
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "pckeiba"
DB_USER = "postgres"
DB_PASS = "postgres"

def get_engine():
    """SQLAlchemy engineを取得"""
    conn_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_string)

def get_connection():
    """データベース接続を取得"""
    engine = get_engine()
    return engine.connect()

def execute_query(query, params=None):
    """SQLクエリを実行し、結果をPandas DataFrameで返す"""
    engine = get_engine()
    if params:
        return pd.read_sql_query(text(query), engine, params=params)
    return pd.read_sql_query(text(query), engine)

def list_tables():
    """データベース内のテーブル一覧を取得"""
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.get_table_names()

def get_table_schema(table_name):
    """指定テーブルのスキーマ情報を取得"""
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.get_columns(table_name)

def query_with_cache(query, cache_name, force_refresh=False, params=None):
    """キャッシュ機能付きクエリ実行
    
    大きなクエリを何度も実行するのを避けるためのキャッシュ機能
    """
    cache_dir = os.path.join(os.path.dirname(__file__), '../../data/cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    cache_path = os.path.join(cache_dir, f"{cache_name}.pkl")
    
    if not force_refresh and os.path.exists(cache_path):
        print(f"キャッシュから{cache_name}を読み込み中...")
        return pd.read_pickle(cache_path)
    
    print(f"データベースから{cache_name}を取得中...")
    df = execute_query(query, params)
    df.to_pickle(cache_path)
    return df

if __name__ == "__main__":
    # 動作確認
    tables = list_tables()
    print(f"データベース内のテーブル: {tables}")
    
    # テーブルが存在する場合にスキーマをチェック
    if tables:
        sample_table = tables[0]
        schema = get_table_schema(sample_table)
        print(f"テーブル {sample_table} のスキーマ:")
        for column in schema:
            print(f"  {column['name']}: {column['type']}")
