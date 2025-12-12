import requests
import json
import pandas as pd
from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

team_data = {
    'Year':[], 'Team':[], 'ParkFactor':[]
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

    url = f'https://www.fangraphs.com/tools/guts?season={year}&teamid=0&type=pf&sortcol=2&sortdir=desc'

    response = requests.get(url)
    html = response.text

    soup = bs(html, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    json_data = json.loads(script_tag.string)

    player_list = json_data['props']['pageProps']['dehydratedState']['queries'][0]['state']['data']

    for i, player in enumerate(player_list):
        
        TEAM = player.get('Team')
        PF = round(player.get('Basic (5yr)'),3)

        team_df.loc[count+i]=[year, TEAM, PF]

    count += (i+1)

table_name = 'ballpark_factors'

team_df.to_sql(
    name=table_name,    
    con=engine,         
    if_exists='replace',
                        
                        
                        
    index=False         
)

team_index = {
    'Team':[], 'Abb':[]
}
index_df = pd.DataFrame(team_index)

index_df.loc[0] = ['Angels','LAA']
index_df.loc[1] = ['Orioles','BAL']
index_df.loc[2] = ['Red Sox','BOS']
index_df.loc[3] = ['White Sox','CHW']
index_df.loc[4] = ['Indians','CLE']
index_df.loc[5] = ['Tigers','DET']
index_df.loc[6] = ['Royals','KCR']
index_df.loc[7] = ['Twins','MIN']
index_df.loc[8] = ['Yankees','NYY']
index_df.loc[9] = ['Athletics','OAK']
index_df.loc[10] = ['Athletics','ATH']
index_df.loc[11] = ['Mariners','SEA']
index_df.loc[12] = ['Rays','TBR']
index_df.loc[13] = ['Rangers','TEX']
index_df.loc[14] = ['Blue Jays','TOR']
index_df.loc[15] = ['Diamondbacks','ARI']
index_df.loc[16] = ['Braves','ATL']
index_df.loc[17] = ['Cubs','CHC']
index_df.loc[18] = ['Reds','CIN']
index_df.loc[19] = ['Rockies','COL']
index_df.loc[20] = ['Marlins','MIA']
index_df.loc[21] = ['Astros','HOU']
index_df.loc[22] = ['Dodgers','LAD']
index_df.loc[23] = ['Brewers','MIL']
index_df.loc[24] = ['Nationals','WSN']
index_df.loc[25] = ['Mets','NYM']
index_df.loc[26] = ['Phillies','PHI']
index_df.loc[27] = ['Pirates','PIT']
index_df.loc[28] = ['Cardinals','STL']
index_df.loc[29] = ['Padres','SDP']
index_df.loc[30] = ['Giants','SFG']
index_df.loc[31] = ['Cleveland','CLE']
index_df.loc[32] = ['Guardians','CLE']

table_name = 'team_abb_index'

index_df.to_sql(
    name=table_name,    
    con=engine,         
    if_exists='replace',
                        
                        
                        
    index=False          
)