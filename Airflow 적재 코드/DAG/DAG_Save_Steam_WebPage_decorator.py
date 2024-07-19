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



#%%

@dag(
    dag_id="Save_Steam_Webpage",
    schedule_interval= datetime.timedelta(minutes=5),
    start_date = pendulum.datetime(2024, 7, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout = datetime.timedelta(minutes=480)
)
def Save_Process():
    @task.virtualenv(task_id='get_steam_app_data', requirements=["SQLAlchemy==2.0.0", "pandas==2.2.2", "pymysql==1.1.1", "cryptography==42.0.8"], system_site_packages=False)
    def get_steam_app_list(**kwargs):
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        SQL_PATH = MYSQL_URL_PATH
        engine = create_engine(SQL_PATH)
        
        # 299개의 랜덤 조회
        app_list = pd.read_sql('select * from Steam.Steam_AppList order by RAND() Limit 199', con = engine)
                
        return app_list
    
    # to_sql이 판다스 2.2.2 쓰면 SQL도 2.0써야하는데 Airflow는 SQLAlchemy 1.4 버전 종속 있어서 가상환경으로 싳행
    @task.virtualenv(task_id='get_steam_web_page', requirements=["SQLAlchemy==2.0.0", "pandas==2.2.2", "DateTime==5.5", "pymysql==1.1.1", "aiohttp==3.9.5", "cryptography==42.0.8"], system_site_packages=False, execution_timeout = datetime.timedelta(hours=6))
    def save_steam_app_list(app_details, **kwargs):
        
        import time
        import random
        import asyncio
        import aiohttp
        import datetime
        import logging
        import pandas as pd
        from sqlalchemy import create_engine, text
        from sqlalchemy.dialects.mysql import LONGTEXT
        
        SQL_PATH = MYSQL_URL_PATH
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
            datefmt = '%m/%d/%Y %I:%M:%S %p',
        )
        
        logging.info(f"START PYTHON VIRTUAL_ENV")
        
        # DB Writeer
        def write_db(dataframe):
            NOW = datetime.datetime.now()
            dataframe['CreateAt'] = NOW

            engine = create_engine(SQL_PATH)
                    
            logging.info(f"UPLOAD TEMP TABLE")
            dataframe.to_sql('Temp_WebPage', engine, if_exists='replace', index = False, dtype={'web_page': LONGTEXT})
            
            logging.info(f"DELETE TEMP TABLE")
            with engine.begin() as conn:
                conn.execute(text('delete from Steam_WebPage where appid in (select appid from Temp_WebPage)'))
                
            logging.info(f"UPDATE STEAM TABLE")
            dataframe.to_sql('Steam_WebPage', engine, if_exists='append', index = False, dtype={'web_page': LONGTEXT})
            
            
            
        # Crawl Page
        async def get_page(url, params, cookies):
            wait_time = random.uniform(1, 200) * 0.01  # 1초에서 5초 사이의 랜덤한 시간 대기
            await asyncio.sleep(wait_time)

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, params = params, cookies = cookies) as response:
                        if response.status == 200:
                            return await response.text()
                        else:
                            logging.error(f'No Data Returned : {url}')
                except:
                    logging.error(f'Request Error : {url}')
        
        
        # 단순 실행 역할만 하는 함수
        async def return_page(task):
            line = await asyncio.gather(*batch)
            return line
        
        
        # INIT
        app_list = app_details['appid'].tolist()
        cookies = { 'birthtime': '283993201', 'mature_content': '1' ,'Steam_Language':'english'}
        params = {'cc' : 'us', 'l' : 'us'}
                
        # 특정 함수를 병렬로 실행할 task 생성
        tasks = [get_page(url = f"https://store.steampowered.com/app/{str(appid)}", params = params, cookies = cookies) for appid in app_list]
        results = []
        
        # 10개 페이지 동시 크롤링 + 2초 휴식
        BATCH_SIZE = 16
        SLEEP_TIME = 10
        
        for i in range(0, len(tasks), BATCH_SIZE):
            # 배치 ID + 배치 요청 URL
            batch_id = app_list[i:i + BATCH_SIZE]
            batch = tasks[i:i + BATCH_SIZE]
            
            # 배치를 Return 함수에 넣어서 반환. > Airflow 최상단 함수는 비동기 함수가 될 수 없다.
            results = asyncio.run(return_page(batch))
            
            # 데이터프레임 만들기
            df_steam_web_page = pd.DataFrame([(i, v) for i, v in zip(batch_id, results)], columns = ['appid', 'web_page'])
            
            # 배치 업로드
            write_db(df_steam_web_page)
            logging.info(f"Batch {i // BATCH_SIZE + 1} UPLOAD complete (total : {(len(app_list) // BATCH_SIZE) + 1}), sleeping for {SLEEP_TIME} seconds")
            if i + BATCH_SIZE < len(tasks):
                time.sleep(SLEEP_TIME)

        
    # 작업 순서 함수 작성
    app_list = get_steam_app_list()
    save_steam_app_list(app_list)


dag = Save_Process()