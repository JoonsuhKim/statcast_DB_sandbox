import requests
import json
import pandas as pd
from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

player_data = {
    'Name':[],'Year':[], 'Team':[], 'ERA':[], 'BABIP':[], 'GBP':[], 'xERA':[]
}
player_df = pd.DataFrame(player_data)

team_data = {
    'Team':[], 'Year':[], 'OAA':[], 'FRM':[]
}
team_df = pd.DataFrame(team_data)

db_connection_str = 'mysql+mysqlconnector://root:1234@localhost:3306/YOUR_DB'

try:
    engine = create_engine(db_connection_str)
except Exception as e:
    print(f"데이터베이스 연결 오류: {e}")
    exit()

qual = 50
limit = 20000000
count = 0

for year in range(2016,2026):

    url = f'https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=8&month=0&ind=0&startdate=&enddate=&season1={year}&season={year}&qual={qual}&pageitems={limit}'

    response = requests.get(url)
    html = response.text

    soup = bs(html, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    json_data = json.loads(script_tag.string)

    player_list = json_data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['data']

    for i, player in enumerate(player_list):
        name = player.get('Name')
        
        if name:
            name_soup = bs(name, 'html.parser')
            player_name = name_soup.get_text()
        
        TEAM = player.get('TeamName')
        ERA = round(player.get('ERA'),3)
        BABIP = round(player.get('BABIP'),3)
        GBP = round(player.get('GB%'),3)
        xERA = round(player.get('xERA'),3)

        player_df.loc[count+i]=[player_name, year, TEAM, ERA, BABIP, GBP, xERA]

    count += (i+1)

table_name = 'player_stats_by_year'

player_df.to_sql(
    name=table_name,      # MySQL에 생성될 테이블의 이름
    con=engine,           # 데이터베이스 연결 엔진
    if_exists='replace',  # 만약 테이블이 이미 존재한다면?
                          # 'fail': 에러 발생 (기본값)
                          # 'replace': 기존 테이블 삭제 후 새로 생성
                          # 'append': 기존 테이블에 데이터 추가
    index=False           # pandas의 index(0,1,2...)는 저장하지 않음
)

count = 0

for year in range(2016,2026):

    url = f'https://www.fangraphs.com/leaders/major-league?pos=all&stats=fld&lg=all&qual=y&type=1&season={year}&month=0&season1={year}&ind=0&team=0%2Cts'

    response = requests.get(url)
    html = response.text

    soup = bs(html, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    json_data = json.loads(script_tag.string)

    player_list = json_data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['data']

    for i, player in enumerate(player_list):

        INN = player.get('Inn')
        OAA = round(player.get('OAA')/INN,10)
        FRM = round(player.get('CFraming')/INN,10)
        team_name = player.get('TeamNameAbb')

        team_df.loc[count+i]=[team_name, year, OAA, FRM]

    count += (i+1)

table_name = 'team_def_stats'

team_df.to_sql(
    name=table_name,    
    con=engine,          
    if_exists='replace', 
                        
                        
                       
    index=False          
)