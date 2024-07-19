import os
import re
import parmap
import pandas as pd
import numpy as np
import multiprocessing
from tqdm import tqdm 
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


import warnings
warnings.filterwarnings('ignore')

def get_steam_tag_data(tuple_data):
    true = True
    false = False
    appid, html_text = tuple_data
    
    if (html_text == None) or (type(html_text) != str) or (len(html_text) == 0):
        print('Text Not Found (Appid) : ', appid, type(html_text))
        
        return pd.DataFrame()
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        html_script = soup.find('div', class_ = 'responsive_page_template_content').find_all('script', type='text/javascript')[-1].text.replace('\t','').replace('\n','').replace('\r', '')
        tags = re.findall('\[[^)]+\]', html_script)

        if len(tags) > 0:
            df_temp = pd.DataFrame(eval(tags[0])[0])
            df_temp['appid'] = appid
            return df_temp

        else:
            print('Tag Not Found (Appid) : ', appid)
            return pd.DataFrame()
    except:
        print('Text Parsing ERROR (Appid) : ', appid)
        return pd.DataFrame()
    

