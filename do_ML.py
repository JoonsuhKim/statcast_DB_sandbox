import mysql.connector
from mysql.connector import Error
import pandas as pd
import matplotlib.pyplot as plt

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

pitch_info = {
    'Year':[], 'Name':[], 'ERA':[], 'xERA':[], 'GBP':[], 
    'OAA':[], 'ParkFactor':[], 'OPP':[], 'FRM':[], 'BABIP':[]
}

def connect_fetch(query):
    try:
        with mysql.connector.connect(
            host='localhost',
            user='root',   # DB 사용자 이름
            password='YOUR_PASSWORD', # DB 비밀번호
            database='YOUR_DB' # 접속할 데이터베이스 이름
        ) as connection:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                records = cursor.fetchall()
            
            for row in records:
                for key in pitch_info.keys():
                    # row 딕셔너리에서 pitch_info의 키와 일치하는 값을 찾아 append
                    pitch_info[key].append(row[key])
                
    except Error as e:
        print(f'{e}')

def connect_fetch_simp(query):
    try:
        with mysql.connector.connect(
            host='localhost',
            user='root',   # DB 사용자 이름
            password='1234', # DB 비밀번호
            database='Temp_DB' # 접속할 데이터베이스 이름
        ) as connection:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                records = cursor.fetchall()
        
        return records[0]['avg'], records[0]['std']
                
    except Error as e:
        print(f'{e}')

if __name__ == '__main__':
    query1 = '''
            with div_off as (
                select tos.Year, tb.LG, tb.DIV, SUM(tos.xwOBA) as Total, COUNT(tos.Team) AS TeamCount
                from team_off_stats tos JOIN team_basic_info tb
                ON tos.Team = tb.Team
                GROUP BY tb.DIV, tb.LG, tos.year),
            park_factor as (
                select bf.Year, ta.Abb as Team, bf.ParkFactor
                from ballpark_factors bf JOIN team_abb_index ta
                ON bf.Team = ta.Team),
            team_opp as (
                select tos.Year, tos.Team, ((dof.Total - tos.xwOBA)/(dof.TeamCount - 1)) as opp
                from team_off_stats tos JOIN team_basic_info tb 
                ON tos.Team = tb.Team JOIN div_off dof 
                ON tos.Year = dof.Year AND tb.DIV = dof.DIV AND tb.LG = dof.LG)
            select ps.Year, ps.Name, ps.ERA, ps.xERA, ps.GBP, td.OAA,
                pf.ParkFactor, ROUND(top.opp, 3) as OPP, td.FRM, ps.BABIP
            from player_stats_by_year ps JOIN team_def_stats td
            ON ps.Year = td.Year AND ps.Team = td.Team JOIN park_factor pf
            ON ps.Year = pf.Year AND ps.Team = pf.Team JOIN team_opp top
            ON ps.Year = top.Year AND ps.Team = top.Team
            '''
    connect_fetch(query1)

    pitch_info = pd.DataFrame(pitch_info)

    # GBP
    query1 = '''
            select avg(GBP) as avg, stddev_samp(GBP) as std
            from player_stats_by_year
            '''
    avg_GBP, std_GBP = connect_fetch_simp(query1)

    # OAA
    query1 = '''
            select avg(OAA) as avg, stddev_samp(OAA) as std
            from team_def_stats
            '''
    avg_OAA, std_OAA = connect_fetch_simp(query1)

    # PF
    query1 = '''
            select avg(ParkFactor) as avg, stddev_samp(ParkFactor) as std
            from ballpark_factors
            '''
    avg_PF, std_PF = connect_fetch_simp(query1)

    # OPP
    query1 = '''
            select avg(xwOBA) as avg, stddev_samp(xwOBA) as std
            from team_off_stats
            '''
    avg_OPP, std_OPP = connect_fetch_simp(query1)

    # FRM
    query1 = '''
            select avg(FRM) as avg, stddev_samp(FRM) as std
            from team_def_stats
            '''
    avg_FRM, std_FRM = connect_fetch_simp(query1)

    # BABIP
    query1 = '''
            select avg(BABIP) as avg, stddev_samp(BABIP) as std
            from player_stats_by_year
            '''
    avg_BBP, std_BBP = connect_fetch_simp(query1)

    pitch_data = pitch_info

    pitch_data['GBP'] = (pitch_info['GBP'] - avg_GBP) / std_GBP
    pitch_data['OAA'] = (pitch_info['OAA'] - avg_OAA) / std_OAA
    pitch_data['ParkFactor'] = (pitch_info['ParkFactor'] - avg_PF) / std_PF
    pitch_data['OPP'] = (pitch_info['OPP'] - avg_OPP) / std_OPP
    pitch_data['FRM'] = (pitch_info['FRM'] - avg_FRM) / std_FRM
    pitch_data['BABIP'] = (pitch_info['BABIP'] - avg_BBP) / std_BBP

    features = ['GBP', 'OAA', 'ParkFactor', 'OPP', 'FRM', 'BABIP']
    X = pitch_data[features]
    y = pitch_data['ERA'] - pitch_data['xERA']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    ridge_model = Ridge(alpha=1.0)
    ridge_model.fit(X_train, y_train)

    print("--- 릿지 회귀 모델 평가 (Target: ERA - xERA) ---")

    r2_score = ridge_model.score(X_test, y_test)
    print(f"Test Set R-squared (R²): {r2_score:.4f}")

    y_pred = ridge_model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    print(f"Test Set RMSE: {rmse:.4f}")

    print("\n--- 피처별 계수(영향력) ---")
    coefficients = pd.Series(ridge_model.coef_, index=features).sort_values(ascending=False)
    print(coefficients)