import mysql.connector
from mysql.connector import Error
import pandas as pd
import matplotlib.pyplot as plt

pitch_info = {
    'Year':[], 'Team':[], 'Name':[], 'ERA':[], 'BABIP':[], 'FRM':[]
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

if __name__ == '__main__':
    query1 = '''
            select ps.Year, ps.Team, ps.Name, ps.ERA, ps.BABIP, td.FRM
            from player_stats_by_year ps JOIN team_def_stats td
            ON ps.Team = td.Team AND ps.year = td.year
            ''' #쿼리
    connect_fetch(query1)

    pitch_info = pd.DataFrame(pitch_info)
    median_babip_by_gbp = pitch_info.groupby('FRM')\
                                    .filter(lambda x: len(x) > 6)\
                                    .groupby('FRM')['BABIP'].mean()
    
    plt.figure(figsize=(6,6))
    plt.scatter(x=pitch_info['FRM'],
                y=pitch_info['BABIP'],
                c='skyblue',
                alpha=0.7)
    median_babip_by_gbp.plot(color='red', marker='o', linestyle='')    
    plt.xlabel('FRM')
    plt.ylabel('BABIP')
    plt.grid(False)
    plt.show()

# 디비전 쿼리
# With prev as
#     (select year, LG, avg(xwOBA) as div_average
#     from team_off_stats NATURAL JOIN team_basic_info
#     group by year, LG)
# select ps.Year, ps.Team, ps.Name, ps.ERA, ps.BABIP, prev.div_average as AVG,
#         (select avg(xwOBA)
#         from
#             (select * 
#             from team_basic_info tbit
#             where tbit.Team != ps.Team
# --             and tbit.DIV = tbi.DIV
#             and tbit.LG = tbi.LG) as ex JOIN team_off_stats tos
#             ON ex.Team = tos.Team
#             WHERE tos.year = ps.year) as OPP
# from player_stats_by_year ps JOIN team_basic_info tbi
# ON ps.Team = tbi.Team
# JOIN prev 
# ON prev.year = ps.year and prev.LG = tbi.LG

# 파크팩터 쿼리
# with yearly_pf as (
#     select bf.year, tai.abb, bf.ParkFactor
#     from ballpark_factors bf JOIN team_abb_index tai
#     ON bf.Team = tai.Team)
# select ps.Name, ps.Year, ps.Team, ps.ERA, ps.BABIP, ps.GBP, yp.ParkFactor
# from player_stats_by_year ps JOIN yearly_pf yp
# ON ps.year = yp.year and ps.Team = yp.abb