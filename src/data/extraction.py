"""
JRAデータベースからのデータ抽出機能を提供するモジュール
"""
import pandas as pd
from .database import execute_query, query_with_cache

def get_race_base_info(start_year="2010", end_year="2023", limit=None):
    """レース基本情報を取得
    
    Args:
        start_year: 取得開始年 (default: "2010")
        end_year: 取得終了年 (default: "2023")
        limit: 取得件数上限 (default: None)
        
    Returns:
        DataFrame: レース基本情報
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
    SELECT 
        kaisai_nen, 
        kaisai_tsukihi, 
        keibajo_code, 
        race_bango,
        kyori, 
        track_code, 
        tenko_code, 
        CASE 
            WHEN SUBSTRING(track_code, 1, 1) = '1' THEN babajotai_code_shiba 
            ELSE babajotai_code_dirt 
        END as baba_jotai,
        shusso_tosu
    FROM jvd_ra
    WHERE kaisai_nen BETWEEN :start_year AND :end_year
    ORDER BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    {limit_clause}
    """
    
    return query_with_cache(
        query, 
        f"race_base_{start_year}_{end_year}_{limit}", 
        params={"start_year": start_year, "end_year": end_year}
    )

def get_race_and_horse_data(start_year="2010", end_year="2023", limit=None):
    """レースと出走馬情報を結合して取得
    
    Args:
        start_year: 取得開始年 (default: "2010")
        end_year: 取得終了年 (default: "2023")
        limit: 取得件数上限 (default: None)
        
    Returns:
        DataFrame: レースと出走馬情報
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
    SELECT 
        r.kaisai_nen, 
        r.kaisai_tsukihi, 
        r.keibajo_code, 
        r.race_bango,
        r.kyori, 
        r.track_code, 
        r.tenko_code, 
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba 
            ELSE r.babajotai_code_dirt 
        END as baba_jotai,
        r.shusso_tosu, 
        s.ketto_toroku_bango, 
        TRIM(s.bamei) AS bamei, 
        s.wakuban, 
        s.umaban,
        s.kishu_code, 
        TRIM(s.kishumei_ryakusho) AS kishumei_ryakusho, 
        s.chokyoshi_code, 
        TRIM(s.chokyoshimei_ryakusho) AS chokyoshimei_ryakusho,
        s.barei, 
        s.seibetsu_code, 
        s.bataiju, 
        s.zogen_fugo, 
        s.zogen_sa,
        s.blinker_shiyo_kubun, 
        s.kakutei_chakujun, 
        s.soha_time, 
        s.kohan_3f,
        s.tansho_odds, 
        s.tansho_ninkijun,
        u.seinengappi,
        u.ketto_joho_01a as chichiuma_id, 
        TRIM(u.ketto_joho_01b) as chichiuma_name,
        u.ketto_joho_02a as hahauma_id, 
        TRIM(u.ketto_joho_02b) as hahauma_name
    FROM jvd_ra r
    JOIN jvd_se s ON r.kaisai_nen = s.kaisai_nen 
                   AND r.kaisai_tsukihi = s.kaisai_tsukihi
                   AND r.keibajo_code = s.keibajo_code
                   AND r.race_bango = s.race_bango
    LEFT JOIN jvd_um u ON s.ketto_toroku_bango = u.ketto_toroku_bango
    WHERE r.kaisai_nen BETWEEN :start_year AND :end_year
    ORDER BY r.kaisai_nen, r.kaisai_tsukihi, r.keibajo_code, r.race_bango, s.wakuban
    {limit_clause}
    """
    
    return query_with_cache(
        query, 
        f"race_horse_{start_year}_{end_year}_{limit}", 
        params={"start_year": start_year, "end_year": end_year}
    )

def get_horse_previous_races(horse_id, race_date=None):
    """指定した馬の過去レース結果を取得
    
    Args:
        horse_id: 血統登録番号
        race_date: この日付以前のレースのみ取得 (format: "YYYYMMDD")
        
    Returns:
        DataFrame: 過去レース結果
    """
    date_condition = ""
    params = {"horse_id": horse_id}
    
    if race_date:
        date_condition = "AND (s.kaisai_nen < :year OR (s.kaisai_nen = :year AND s.kaisai_tsukihi <= :date))"
        params["year"] = race_date[:4]
        params["date"] = race_date[4:]
    
    query = f"""
    SELECT 
        s.kaisai_nen || s.kaisai_tsukihi AS race_date,
        s.keibajo_code,
        s.race_bango,
        r.kyori,
        r.track_code,
        CASE 
            WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba 
            ELSE r.babajotai_code_dirt 
        END as baba_jotai,
        s.kishu_code,
        s.chokyoshi_code,
        s.bataiju,
        s.zogen_fugo,
        s.zogen_sa,
        CAST(s.kakutei_chakujun AS INTEGER) AS chakujun,
        CAST(s.soha_time AS INTEGER) AS time,
        CAST(s.kohan_3f AS INTEGER) AS last_3f,
        CAST(s.tansho_ninkijun AS INTEGER) AS popularity,
        CAST(s.tansho_odds AS NUMERIC) / 10.0 AS odds
    FROM jvd_se s
    JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen
                  AND s.kaisai_tsukihi = r.kaisai_tsukihi
                  AND s.keibajo_code = r.keibajo_code
                  AND s.race_bango = r.race_bango
    WHERE s.ketto_toroku_bango = :horse_id
      AND s.kakutei_chakujun ~ '^[0-9]+$'  -- 数字のみの着順
      AND s.kakutei_chakujun NOT IN ('00', '99')  -- 除外・取消を除く
    {date_condition}
    ORDER BY s.kaisai_nen, s.kaisai_tsukihi
    """
    
    cache_key = f"horse_prev_{horse_id}"
    if race_date:
        cache_key += f"_{race_date}"
    
    return query_with_cache(query, cache_key, params=params)

def get_jockey_course_stats(jockey_code, years_range=5, min_races=3):
    """騎手のコース別成績を取得
    
    Args:
        jockey_code: 騎手コード
        years_range: 直近何年分のデータを取得するか (default: 5)
        min_races: 最低出走数 (default: 3)
        
    Returns:
        DataFrame: 騎手のコース別成績
    """
    query = """
    WITH jockey_courses AS (
        SELECT 
            s.kishu_code,
            r.keibajo_code,
            SUBSTRING(r.track_code, 1, 1) AS track_type,
            CASE 
                WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
                WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
                ELSE '長距離'
            END AS distance_category,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1) AS wins,
            COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) <= 3) AS top3,
            ROUND(COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1)::NUMERIC / COUNT(*) * 100, 2) AS win_rate,
            ROUND(COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) <= 3)::NUMERIC / COUNT(*) * 100, 2) AS top3_rate,
            ROUND(AVG(CAST(s.tansho_odds AS NUMERIC) / 10.0) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1), 2) AS avg_win_odds,
            ROUND(SUM(CASE WHEN CAST(s.kakutei_chakujun AS INTEGER) = 1 THEN CAST(s.tansho_odds AS NUMERIC) / 10.0 ELSE 0 END) / COUNT(*) * 100, 2) AS roi
        FROM jvd_se s
        JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen
                      AND s.kaisai_tsukihi = r.kaisai_tsukihi
                      AND s.keibajo_code = r.keibajo_code
                      AND s.race_bango = r.race_bango
        WHERE s.kishu_code = :jockey_code
          AND CAST(s.kaisai_nen AS INTEGER) >= EXTRACT(YEAR FROM CURRENT_DATE) - :years_range
          AND s.kakutei_chakujun ~ '^[0-9]+$'
          AND s.kakutei_chakujun NOT IN ('00', '99')
        GROUP BY s.kishu_code, r.keibajo_code, track_type, distance_category
        HAVING COUNT(*) >= :min_races
    )
    SELECT 
        kishu_code,
        keibajo_code,
        track_type,
        distance_category,
        total_races,
        wins,
        top3,
        win_rate,
        top3_rate,
        avg_win_odds,
        roi,
        CASE 
            WHEN roi >= 200 THEN 'S'
            WHEN roi >= 100 THEN 'A'
            WHEN roi >= 70 THEN 'B'
            WHEN roi >= 50 THEN 'C'
            ELSE 'D'
        END AS roi_rank
    FROM jockey_courses
    ORDER BY roi DESC
    """
    
    return execute_query(query, params={
        "jockey_code": jockey_code, 
        "years_range": years_range,
        "min_races": min_races
    })

def get_sire_track_condition_stats(sire_id=None, min_horses=3, min_races=10):
    """種牡馬の馬場状態別成績を取得
    
    Args:
        sire_id: 種牡馬ID (指定しない場合は全種牡馬)
        min_horses: 最低必要な産駒数 (default: 3)
        min_races: 最低出走数 (default: 10)
        
    Returns:
        DataFrame: 種牡馬の馬場状態別成績
    """
    sire_condition = "AND u.ketto_joho_01a = :sire_id" if sire_id else ""
    params = {"min_horses": min_horses, "min_races": min_races}
    
    if sire_id:
        params["sire_id"] = sire_id
    
    query = f"""
    WITH sire_condition_stats AS (
        SELECT 
            u.ketto_joho_01a AS sire_id,
            TRIM(u.ketto_joho_01b) AS sire_name,
            SUBSTRING(r.track_code, 1, 1) AS track_type,
            CASE 
                WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba
                ELSE r.babajotai_code_dirt
            END AS track_condition,
            COUNT(DISTINCT s.ketto_toroku_bango) AS horses_count,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1) AS wins,
            ROUND(COUNT(*) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1)::NUMERIC / COUNT(*) * 100, 2) AS win_rate,
            ROUND(AVG(CAST(s.tansho_ninkijun AS INTEGER)), 2) AS avg_popularity,
            ROUND(AVG(CAST(s.tansho_odds AS NUMERIC) / 10.0) FILTER (WHERE CAST(s.kakutei_chakujun AS INTEGER) = 1), 2) AS avg_win_odds,
            ROUND(SUM(CASE WHEN CAST(s.kakutei_chakujun AS INTEGER) = 1 THEN CAST(s.tansho_odds AS NUMERIC) / 10.0 ELSE 0 END) / COUNT(*) * 100, 2) AS roi
        FROM jvd_se s
        JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen
                      AND s.kaisai_tsukihi = r.kaisai_tsukihi
                      AND s.keibajo_code = r.keibajo_code
                      AND s.race_bango = r.race_bango
        JOIN jvd_um u ON s.ketto_toroku_bango = u.ketto_toroku_bango
        WHERE s.kakutei_chakujun ~ '^[0-9]+$'
          AND s.kakutei_chakujun NOT IN ('00', '99')
          {sire_condition}
        GROUP BY u.ketto_joho_01a, sire_name, track_type, track_condition
        HAVING COUNT(DISTINCT s.ketto_toroku_bango) >= :min_horses
           AND COUNT(*) >= :min_races
    )
    SELECT 
        sire_id,
        sire_name,
        track_type,
        CASE 
            WHEN track_condition = '1' THEN '良'
            WHEN track_condition = '2' THEN '稍重'
            WHEN track_condition = '3' THEN '重'
            WHEN track_condition = '4' THEN '不良'
            ELSE track_condition
        END AS track_condition,
        horses_count,
        total_races,
        wins,
        win_rate,
        avg_popularity,
        avg_win_odds,
        roi,
        CASE 
            WHEN roi >= 200 THEN 'S'
            WHEN roi >= 100 THEN 'A'
            WHEN roi >= 70 THEN 'B'
            WHEN roi >= 50 THEN 'C'
            ELSE 'D'
        END AS roi_rank
    FROM sire_condition_stats
    ORDER BY roi DESC
    """
    
    cache_key = "sire_track_condition"
    if sire_id:
        cache_key += f"_{sire_id}"
        
    return query_with_cache(query, cache_key, params=params)

if __name__ == "__main__":
    # 動作テスト用コード
    print("レース基本情報のサンプル取得")
    races = get_race_base_info(limit=5)
    print(races.head())
    
    print("\nレースと出走馬情報のサンプル取得")
    race_horse = get_race_and_horse_data(limit=5)
    print(race_horse.head())
