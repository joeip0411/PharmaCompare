import os
import time

import cloudscraper
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from supabase import create_client

DOMAIN = 'https://www.chemistwarehouse.com.au'
SUPABASE_URL = os.getenv('supabase_url')
SUPBASE_KEY = os.getenv('supabase_key')
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

def scrape_product_url():

    driver = webdriver.Chrome()
    category_urls = get_category_url()
    product_urls = set()

    for d in category_urls:
        url = d['url']
        
        driver.get(url=url)

        while True:
            elements = driver.find_elements(By.XPATH, '//a[@data-analytics-sku]')

            for element in elements:
                href = element.get_attribute('href')
                product_urls.add(href)
            
            time.sleep(1)

            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, 'button.pager__button.pager__button--next')
                next_btn.click()
                time.sleep(1)
            except:
                break
    
    res = list(product_urls)

    return res

def upload_product_url():
    product_url = scrape_product_url()
    url_kv = [{'url': url} for url in product_url]

    supabase = SUPABASE_CLIENT

    response = supabase.table('l_product_url')\
        .upsert(url_kv)\
        .execute()
    
    return response.data


def get_product_details():

    supabase = SUPABASE_CLIENT
    scraper = CLOUD_SCRAPER

    res = []

    query = """

    select
        url
    from
        public.l_product_url
    where
        data_id not in (
            select
                data_id
            from
                public.product
    )

    """
    response = supabase.rpc('run_custom_query', {'query_text': query})\
        .execute()

    urls = [i['result'] for i in response.data]

    for url in urls:
        response = scraper.get(url)
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        product_info = {}

        try:
            data_id = url.split('/')[4]
            product_id = soup.find('div', class_='product-id').contents[0].split(': ')[1]
            product_name = soup.find('div', class_='product-name').contents[1].contents[0].strip()

            product_info['data_id'] = data_id
            product_info['product_id'] = product_id
            product_info['product_name'] = product_name

            cols = ['image_url', 'description', 'general_info', 'ingredients', 
                    'directions', 'warnings', 'miscellaneous', 'druginteractions', 
                    'commonuses', 'indications']

            for c in cols:
                product_info[c] = ''

            sections = soup.findAll('section', {'class':'product-info-section'})
            filtered_sections = [section for section in sections if 'hidden' not in section.get('class', [])]

            for s in filtered_sections:
                section_name = s.get('class', [])[1].replace('-', '_')
                section_conent = s.find('div').contents
                section_conent_text = '\n'.join([i.text for i in section_conent]).strip()
                product_info[section_name] = section_conent_text
            
            if product_info:
                res.append(product_info)

        except:
            pass
        print(len(res))
    
    return res

def upload_product_details(product_details:dict):
    supabase = SUPABASE_CLIENT

    response = supabase.table('product')\
        .upsert(product_details)\
        .execute()

    return response