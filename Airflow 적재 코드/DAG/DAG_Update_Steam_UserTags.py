import re
import parmap
import logging
import datetime
import warnings
import numpy as np
import pandas as pd
import multiprocessing
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')

# INIT 
import sys
sys.path.append('.')
import preprocessing

SQL_PATH = MYSQL_URL_PATH

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
    datefmt = '%m/%d/%Y %I:%M:%S %p',
)

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import get_current_context


#%%

@dag(
    dag_id="Update_Steam_Tags",
    schedule_interval="10 * * * *",
    start_date = pendulum.datetime(2024, 7, 9, tz="UTC"),
    catchup=False,
    dagrun_timeout = datetime.timedelta(minutes=480)
)
def Save_Process():
    @task.virtualenv(task_id='get_steam_webpage', requirements=["DateTime==5.5", "SQLAlchemy==2.0.0", "pandas==2.2.2", "pymysql==1.1.1", "cryptography==42.0.8"], system_site_packages=False)
    def get_steam_web_page(**kwargs):
        import logging
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
            datefmt = '%m/%d/%Y %I:%M:%S %p',
        )

        import pandas as pd
        from sqlalchemy import create_engine, text
        
        SQL_PATH = MYSQL_URL_PATH
        engine = create_engine(SQL_PATH)

        import datetime
        batch_num = datetime.datetime.now().hour % 10

        logging.info(f"MAKE SQL : {batch_num} BATCH QUERYED")
        sql = f"""
        WITH BATCH_NUM AS (
        SELECT appid, RIGHT(ROUND(appid / 10), 1) AS NUM
        FROM Steam_WebPage
        )

        SELECT *
        FROM Steam_WebPage
        WHERE appid IN (SELECT appid FROM BATCH_NUM WHERE NUM = {str(batch_num)})
        """
        logging.info(f"Read DB : Steam WebPage")

        web_page = pd.read_sql(sql, con = engine)
        logging.info(f"Read Complete DB : Steam WebPage")

        return web_page
    
    @task(task_id='html_parsing')
    def parse_html(web_page, **kwargs):
        num_cores = max(multiprocessing.cpu_count() - 1, 1)
        results = parmap.map(preprocessing.get_steam_tag_data, web_page['appid'].tolist(), web_page['web_page'].tolist(), pm_pbar=True, pm_processes=num_cores)
        results = pd.concat(results, axis = 0).reset_index(drop=True)
        return results


    # to_sql이 판다스 2.2.2 쓰면 SQL도 2.0써야하는데 Airflow는 SQLAlchemy 1.4 버전 종속 있어서 가상환경으로 싳행
    @task.virtualenv(task_id='write_steam_tags_db', requirements=["SQLAlchemy==2.0.0", "pandas==2.2.2", "DateTime==5.5", "pymysql==1.1.1", "aiohttp==3.9.5", "cryptography==42.0.8"], system_site_packages=False, execution_timeout = datetime.timedelta(hours=6))
    def upload_steam_tags(tag_list, **kwargs):
        import logging
        import datetime
        import pandas as pd
        from sqlalchemy import create_engine, text
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
            datefmt = '%m/%d/%Y %I:%M:%S %p',
        )

        SQL_PATH = MYSQL_URL_PATH
        engine = create_engine(SQL_PATH)

        # 수정 날짜 APPEND
        NOW = datetime.datetime.now()
        tag_list['CreateAt'] = NOW

        # Upload
        logging.info(f"Temp_Tags UPLOAD START")
        tag_list.to_sql('Temp_Tags', engine, if_exists='replace', index = False)
        logging.info(f"Steam_Tags DELETE COMPLETE")
        with engine.begin() as conn:
            conn.execute(text('delete from Steam_Tags where appid in (select appid from Temp_Tags)'))
        logging.info(f"Steam_Tags UPLOAD COMPLETE")
        tag_list.to_sql('Steam_Tags', engine, if_exists='append', index = False)
        
        
    # 작업 순서 함수 작성
    web_page = get_steam_web_page()
    tag_list = parse_html(web_page)
    upload_steam_tags(tag_list)


dag = Save_Process()