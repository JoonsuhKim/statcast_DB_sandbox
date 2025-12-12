import requests
import json
import pandas as pd
from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

player_data = {
    'Name':[],'K9':[],'BB9':[],'HR9':[], 'BABIP':[], 'FIP':[], 'GBR':[], 
    'EV':[], 'LA':[], 'BarrelR':[], 'HardHitR':[], 'xERA':[], 'ERA':[], 'GAP':[]
}
player_df = pd.DataFrame(player_data)

db_connection_str = 'mysql+mysqlconnector://root:1234@localhost:3306/Baseball_Stats_pitcher'

try:
    engine = create_engine(db_connection_str)
except Exception as e:
    print(f"데이터베이스 연결 오류: {e}")
    exit()

start = 2025
end = 2025
qual = 10
limit = 20000000

url = f'https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=8&month=0&ind=0&startdate=&enddate=&season1={start}&season={end}&qual={qual}&pageitems={limit}'

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
    
    ERA = round(player.get('ERA'),3)
    IP = round(player.get('IP'),3)
    K9 = round(player.get('K/9'),3)
    BB9 = round(player.get('BB/9'),3)
    HR9 = round(player.get('HR/9'),3)
    BABIP = round(player.get('BABIP'),3)
    FIP = round(player.get('FIP'),3)
    GBR = round(player.get('GB%'),3)
    xERA = round(player.get('xERA'),3)
    EV = round(player.get('EV'),3)
    LA = round(player.get('LA'),3)
    BARRELR = round(player.get('Barrel%'),3)
    HardHitR = round(player.get('HardHit%')*100,3)
    GAP = xERA - ERA

    player_df.loc[i]=[player_name, K9, BB9, HR9, BABIP, FIP, GBR,
                      EV, LA, BARRELR, HardHitR, xERA, ERA, GAP]
    
table_name = 'player_stats_temp'

player_df.to_sql(
    name=table_name,      
    con=engine,           
    if_exists='replace',  
                         
                        
                         
    index=False          
)

# print(player_df[(player_df['ERA']-player_df['xERA'])<=0])

# condition1 = (player_df['ERA']-player_df['FIP']>0)&(player_df['ERA']<3.5)
# # condition1_1 = (player_df['ERA']-player_df['xERA']>=0)&(player_df['ERA']>=3.5)
# condition2 = (player_df['ERA']+0<player_df['FIP'])&(player_df['ERA']<3.5)
# # condition2_1 = (player_df['ERA']+0<=player_df['xERA'])&(player_df['ERA']>=3.5)
# over_df = player_df[condition1]
# # over_un = player_df[condition1_1]
# under_df = player_df[condition2]
# # under_un = player_df[condition2_1]

# plt.figure(figsize=(6,6))
# plt.scatter(x=over_df['GAP'],
#             y=over_df['BABIP'],
#             c='red',
#             alpha=0.8)
# # plt.scatter(x=over_un['GAP'],
# #             y=over_un['BABIP'],
# #             c='green',
# #             alpha=0.2)
# plt.scatter(x=under_df['GAP'],
#             y=under_df['BABIP'],
#             c='blue',
#             alpha=0.8)
# # plt.scatter(x=under_un['GAP'],
# #             y=under_un['BABIP'],
# #             c='purple',
# #             alpha=0.2)
# plt.xlabel('GAP')
# plt.ylabel('BABIP')
# plt.grid(False)
# plt.show()