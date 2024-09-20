import os
import time

import cloudscraper
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from supabase import create_client

DOMAIN = 'https://www.chemistwarehouse.com.au'
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPBASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_CLIENT = create_client(supabase_url=SUPABASE_URL, supabase_key=SUPBASE_KEY)
CLOUD_SCRAPER = cloudscraper.create_scraper()

def scrape_category_url():
    scraper = CLOUD_SCRAPER
    response = scraper.get(DOMAIN + '/categories')

    content = response.text
    soup = BeautifulSoup(content, 'html.parser')

    rows = soup.find_all('tr')
    res = []

    # find all top level categories url
    for row in rows:
        divs = row.find_all('div', style="width:20px;height:1px")
        if len(divs) == 1 and len(row.find_all('div')) == 1:
            a_tag = row.find('td', style="white-space:nowrap;").find('a', href=True)
            path = a_tag['href']
            res.append(DOMAIN + path)

    return res

def upload_category_url():
    cat_url = scrape_category_url()
    url_kv = [{'url': url} for url in cat_url]

    supabase = SUPABASE_CLIENT

    response = supabase.table('l_category_url')\
        .upsert(url_kv)\
        .execute()
    
    return response.data

def get_category_url():

    supabase = SUPABASE_CLIENT

    response = supabase.table('l_category_url')\
        .select('url')\
        .execute()
    
    return response.data