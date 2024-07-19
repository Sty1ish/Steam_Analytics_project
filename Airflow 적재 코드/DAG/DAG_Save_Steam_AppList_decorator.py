import os
import json
import time
import random
import logging
import aiohttp
import asyncio
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text

API_KEY = STEAM_WEB_API_KEY
SQL_PATH = MYSQL_URL_PATH

# 레퍼런스 : https://gunuuu.tistory.com/38

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
    datefmt = '%m/%d/%Y %I:%M:%S %p',
)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import get_current_context


@dag(
    dag_id="Save_Steam_AppList",
    schedule_interval="0 0 * * *",
    start_date = pendulum.datetime(2024, 7, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout = datetime.timedelta(minutes=60),
)
def Save_Process():
    @task(task_id='get_steam_app_data')
    def get_steam_app_list(**kwargs):
        game_list_v2 = []
        last_appid = ''
        have_more_results = True

        while have_more_results:
            url = 'https://api.steampowered.com/IStoreService/GetAppList/v1/'
            params = {'key' : API_KEY, 'max_results' : 50000, 'last_appid' : last_appid}

            data = requests.get(url, params = params)

            temp_json = json.loads(data.text)

            if 'have_more_results' in temp_json['response'].keys():
                have_more_results = temp_json['response']['have_more_results']
            else:
                have_more_results = False

            if 'last_appid' in temp_json['response'].keys():
                last_appid = temp_json['response']['last_appid']

            game_list_v2.append(pd.DataFrame(temp_json['response']['apps']))
            logging.info(f'page {len(game_list_v2)} Crwaled')
        
        df_game_list = pd.concat(game_list_v2, axis = 0).reset_index(drop=True)
        logging.info(f'## ALL PAGE CRAWLED ##')
        return df_game_list
        # # return한 값은 자동으로 xcom에 key='return_value', task_ids=task_id로 저장
        # https://letzgorats.tistory.com/entry/Airflow-Python-Operator%EC%97%90%EC%84%9C-Xcom-%EC%82%AC%EC%9A%A9
    
    # to_sql이 판다스 2.2.2 쓰면 SQL도 2.0써야하는데 Airflow는 SQLAlchemy 1.4 버전 종속 있어서 가상환경으로 싳행
    @task.virtualenv(task_id='save_steam_app_data', requirements=["SQLAlchemy==2.0.0", "pandas==2.2.2", "DateTime==5.5", "pymysql==1.1.1", "cryptography==42.0.8"], system_site_packages=False)
    def save_steam_app_list(app_data, **kwargs):
        # INIT - GLOBAL SETTING이지만, 가상 환경이라서 다시 정의해야함.
        import datetime
        import logging
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        # DB 서버 자체에서 돌아가는 코드임.
        SQL_PATH = MYSQL_URL_PATH
        
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
            datefmt = '%m/%d/%Y %I:%M:%S %p',
        )
        # INIT 종료
        
        NOW = datetime.datetime.now()
        app_data['CreateAt'] = NOW

        engine = create_engine(SQL_PATH)
                
        app_data.to_sql('Temp_AppList', engine, if_exists='replace', index = False)
        logging.info(f'## TEMP TABLE UPLOAD COMPLETE ##')
        with engine.begin() as conn:
            conn.execute(text('delete from Steam_AppList where appid in (select appid from Temp_AppList)'))
        logging.info(f'## ORIGINAL TABLE DELETE COMPLETE ##')
        app_data.to_sql('Steam_AppList', engine, if_exists='append', index = False)
        logging.info(f'## ORIGINAL TABLE APPEND COMPLETE ##')
    
    # 작업 순서 함수 작성
    # 2.0에서 변경된 코드 : https://medium.com/fenderi/airflow-v2-0-0-%EB%B3%80%EA%B2%BD%EC%A0%90-%EC%A0%95%EB%A6%AC-76aea878e555
    app_data = get_steam_app_list()
    save_steam_app_list(app_data)
    
    # 명시적 용어 제외
    # get_steam_app_list() >> save_steam_app_list()

dag = Save_Process()