import requests
import json
import pandas as pd
from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

player_data = {
    'Team':[],'Year':[], 'xwOBA':[], 'wRC+':[]
}
player_df = pd.DataFrame(player_data)

team_data = {
    'Team':[], 'LG':[], 'DIV':[]
}
team_df = pd.DataFrame(team_data)

db_connection_str = 'mysql+mysqlconnector://root:1234@localhost:3306/YOUR_DB'

try:
    engine = create_engine(db_connection_str)
except Exception as e:
    print(f"데이터베이스 연결 오류: {e}")
    exit()

count = 0

for year in range(2016,2026):

    url = f'https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&qual=0&type=8&season={year}&month=0&season1={year}&ind=0&team=0,ts&rost=&age=&filter=&players=0'

    response = requests.get(url)
    html = response.text

    soup = bs(html, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    json_data = json.loads(script_tag.string)

    player_list = json_data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']['data']

    for i, player in enumerate(player_list):
        name = player.get('Team')
        
        if name:
            name_soup = bs(name, 'html.parser')
            player_name = name_soup.get_text()

        xwOBA = round(player.get('xwOBA'),3)
        wRCpl = round(player.get('wRC+'),3)

        player_df.loc[count+i]=[player_name, year, xwOBA, wRCpl]

    count += (i+1)

table_name = 'team_off_stats'

player_df.to_sql(
    name=table_name,      # MySQL에 생성될 테이블의 이름
    con=engine,           # 데이터베이스 연결 엔진
    if_exists='replace',  # 만약 테이블이 이미 존재한다면?
                          # 'fail': 에러 발생 (기본값)
                          # 'replace': 기존 테이블 삭제 후 새로 생성
                          # 'append': 기존 테이블에 데이터 추가
    index=False           # pandas의 index(0,1,2...)는 저장하지 않음
)

url = f'https://www.fangraphs.com/leaders/major-league?pos=all&stats=bat&lg=all&qual=0&type=8&season=2025&month=0&season1=2025&ind=0&team=0,ts&rost=&age=&filter=&players=0'

response = requests.get(url)
html = response.text

soup = bs(html, 'html.parser')
script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

json_data = json.loads(script_tag.string)

player_list = json_data['props']['pageProps']['dehydratedState']['queries'][3]['state']['data']

for i, player in enumerate(player_list):
    
    LG = player.get('League')
    team_name = player.get('AbbName')

    team_df.loc[i]=[team_name, LG, '']

team_df.loc[i+1]=['OAK', 'AL', '']

team_df.loc[0, 'DIV'] = 'West' #LAA
team_df.loc[1, 'DIV'] = 'West' #HOU
team_df.loc[2, 'DIV'] = 'West' #ATH
team_df.loc[3, 'DIV'] = 'East' #TOR
team_df.loc[4, 'DIV'] = 'East' #ATL
team_df.loc[5, 'DIV'] = 'Cent' #MIL
team_df.loc[6, 'DIV'] = 'Cent' #STL
team_df.loc[7, 'DIV'] = 'Cent' #CHC
team_df.loc[8, 'DIV'] = 'West' #ARI
team_df.loc[9, 'DIV'] = 'West' #LAD
team_df.loc[10, 'DIV'] = 'West' #SFG
team_df.loc[11, 'DIV'] = 'Cent' #CLE
team_df.loc[12, 'DIV'] = 'West' #SEA
team_df.loc[13, 'DIV'] = 'East' #MIA
team_df.loc[14, 'DIV'] = 'East' #NYM
team_df.loc[15, 'DIV'] = 'East' #WSN
team_df.loc[16, 'DIV'] = 'East' #BAL
team_df.loc[17, 'DIV'] = 'West' #SDP
team_df.loc[18, 'DIV'] = 'East' #PHI
team_df.loc[19, 'DIV'] = 'Cent' #PIT
team_df.loc[20, 'DIV'] = 'West' #TEX
team_df.loc[21, 'DIV'] = 'East' #TBR
team_df.loc[22, 'DIV'] = 'East' #BOS
team_df.loc[23, 'DIV'] = 'Cent' #CIN
team_df.loc[24, 'DIV'] = 'West' #COL
team_df.loc[25, 'DIV'] = 'Cent' #KCR
team_df.loc[26, 'DIV'] = 'Cent' #DET
team_df.loc[27, 'DIV'] = 'Cent' #MIN
team_df.loc[28, 'DIV'] = 'Cent' #CHW
team_df.loc[29, 'DIV'] = 'East' #NYY
team_df.loc[30, 'DIV'] = 'West' #OAK

table_name = 'team_basic_info'

team_df.to_sql(
    name=table_name,    
    con=engine,           
    if_exists='replace', 
                          
                          
                          
    index=False           
)