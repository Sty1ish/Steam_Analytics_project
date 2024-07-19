import os
import json
import time
import logging
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

API_KEY = STEAM_WEB_API_KEY
SQL_PATH = MYSQL_URL_PATH
engine = create_engine(SQL_PATH)
       

start_user = 76561197960265729
end_user = 76561199683856343

# 임시 작업용
# target_user_id = 76561199131256224


logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
    datefmt = '%m/%d/%Y %I:%M:%S %p',
)

logging.info(f"START USER CRAWLING")
counter = 0

# 무한 반복
while True:
    # Make User
    target_user_id = int(np.random.uniform(start_user, end_user))
    # logging.info(f"SAMPLEING USER : {target_user_id}")
    counter += 1
    if counter % 100 == 0:
        logging.info(f"SAMPLEING USER AMOUNT : {counter}")
    
    
    # Find User
    url = 'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/'
    params = {'key' : API_KEY, 'steamids' : str(target_user_id)}
    r = requests.get(url, params = params)

    if r.status_code != 200:
        print(r.text)
        raise 'Value_ERROR'

    try:
        json_temp = json.loads(r.text)
        json_temp = json_temp['response']
        df_user_temp = pd.DataFrame(json_temp['players'])
        df_user_temp['steamid'] = target_user_id
    except:
        df_user_temp = pd.DataFrame()
        df_user_temp['steamid'] = target_user_id
    
    if len(df_user_temp) > 0:
        df_user_temp.to_sql('Temp_User', engine, if_exists='replace', index = False)

        with engine.begin() as conn:
            conn.execute(text('delete from Steam_User where steamid in (select steamid from Temp_User)'))
            
        df_user_temp.to_sql('Steam_User', engine, if_exists='append', index = False)
    

    # Collect Game Playtime
    url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/'
    params = {'key' : API_KEY, 'steamid' : target_user_id}
    r = requests.get(url, params = params)

    if r.status_code != 200:
        print(r.text)
        raise 'Value_ERROR'

    try:
        json_temp = json.loads(r.text)
        json_temp = json_temp['response']
        df_playtime = pd.DataFrame(json_temp['games'])
        df_playtime['steamid'] = target_user_id
    except:
        df_playtime = pd.DataFrame()
        
    if len(df_playtime) > 0:
        df_playtime.to_sql('Temp_Playtime', engine, if_exists='replace', index = False)

        with engine.begin() as conn:
            conn.execute(text('delete from Steam_Playtime where steamid in (select steamid from Temp_Playtime)'))
            
        df_playtime.to_sql('Steam_Playtime', engine, if_exists='append', index = False)
        
    time.sleep(1.5)
