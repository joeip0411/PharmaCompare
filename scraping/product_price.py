from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from util import *


def get_selenium_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920x1080')

    chromedriver_path = '/usr/bin/chromedriver'
    service = Service(chromedriver_path)
    # driver = webdriver.Chrome(options=options)
    driver = webdriver.Chrome(service=service, options=options)

    return driver

def get_product_prices(driver, category_urls):

    res = set()

    for d in category_urls:
        url = d['url']
        driver.get(url)

        while True:
            
            product_elements = driver.find_elements(By.CLASS_NAME, 'category-product')

            for element in product_elements:
                product_sku = element.get_attribute('data-analytics-sku')
                product_price = element.get_attribute('data-analytics-price')
                product_price = (product_sku, product_price)
                res.add(product_price)
            time.sleep(1)

            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, 'button.pager__button.pager__button--next')
            except:
                break
            
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(2)
 

    kv = [{'data_id':i[0], 'price':i[1]} for i in res]

    return kv

def upload_product_prices(product_prices:list[dict]):
    
    supabase = SUPABASE_CLIENT

    supabase.table('price')\
        .upsert(product_prices)\
        .execute()
    
    return True
    

driver = get_selenium_driver()
category_urls = get_category_url()
product_prices = get_product_prices(driver=driver, category_urls=category_urls)
upload_product_prices(product_prices)