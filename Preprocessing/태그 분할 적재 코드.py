import os
import re
import sys
import parmap
import datetime
import pandas as pd
import numpy as np
import multiprocessing
from tqdm import tqdm 
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


os.chdir(r'C:\Users\Nyoths\Desktop\넥슨\Steam 분석 프로젝트\정기수집 코드 작성(DAG)')
sys.path.append('.')

import preprocessing
import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level = logging.INFO, # 시끄러우면 얘가 문제임 > 개발 끝나면 디버그를 ERROR로 변경할것.
    datefmt = '%m/%d/%Y %I:%M:%S %p',
)

def get_batch_data(batch_num):
    SQL_PATH = MYSQL_URL_PATH

    if (batch_num < 0) or (batch_num > 9):
        raise '0 <= batch_num <= 9'
    engine = create_engine(SQL_PATH)

    sql = f"""
    WITH BATCH_NUM AS (
    SELECT appid, RIGHT(ROUND(appid / 10), 1) AS NUM
    FROM Steam_WebPage
    )

    SELECT *
    FROM Steam_WebPage
    WHERE appid IN (SELECT appid FROM BATCH_NUM WHERE NUM = {str(batch_num)})
    """

    data = pd.read_sql(sql, con = engine)
    return data


if __name__ == "__main__":
    num_cores = multiprocessing.cpu_count()   
    SQL_PATH = MYSQL_URL_PATH
    engine = create_engine(SQL_PATH)

    for i in range(0,10):
        data = get_batch_data(i)
        logging.info(f"MAKE SQL : {i} BATCH QUERYED")
        tuple_data = list(zip(data['appid'].tolist(), data['web_page'].tolist()))

        results = parmap.map(preprocessing.get_steam_tag_data, tuple_data, pm_pbar=True, pm_processes=num_cores)
        results = pd.concat(results, axis = 0).reset_index(drop=True)
        NOW = datetime.datetime.now()
        results['CreateAt'] = NOW

        if i == 0:
            results.to_sql('Steam_Tags', engine, if_exists='replace', index = False)
        else:
            results.to_sql('Steam_Tags', engine, if_exists='append', index = False)
            
    logging.info(f"ALL ACTION END")
