import csv
import io
import os
import time
from datetime import datetime, timezone

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from util import *

SINK_TABLE = os.getenv('PRODUCT_PRICE_TABLE')
S3_PRICE_RAW_BUCKET = os.getenv('S3_PRICE_RAW_BUCKET')

def get_selenium_driver():
    options = webdriver.ChromeOptions()
    option_arg = ["--no-sandbox", "--headless", "--disable-gpu", 
                  '--disable-dev-shm-usage', '--window-size=1920x1080']

    for arg in option_arg:
        options.add_argument(arg)

    chromedriver_path = '/usr/bin/chromedriver'
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    return driver

def get_product_prices(driver, category_urls):

    res = set()

    for d in category_urls:
        url = d['url']
        driver.get(url)

        print('At ' + url, flush=True)
        page = 1

        while True:
            print('In page', page)
            time.sleep(2)
            product_elements = driver.find_elements(By.CLASS_NAME, 'category-product')

            for element in product_elements:
                product_sku = element.get_attribute('data-analytics-sku')
                product_price = element.get_attribute('data-analytics-price')
                product_price = (product_sku, product_price)
                res.add(product_price)
            time.sleep(2)

            try:
                next_btn = driver.find_element(
                    By.CSS_SELECTOR, 'button.pager__button.pager__button--next')
            except:
                break
            
            driver.execute_script("arguments[0].click();", next_btn)
            page += 1
 

    kv = [{'data_id':i[0], 'price':i[1]} for i in res]

    return kv

def upload_price_to_s3(product_prices:list[dict], date:str):
    with io.StringIO() as csv_buffer:
        # Write the list of dictionaries to the CSV buffer
        writer = csv.DictWriter(csv_buffer, fieldnames=product_prices[0].keys())
        writer.writeheader()
        writer.writerows(product_prices)
        
        # Move the buffer cursor to the beginning
        csv_buffer.seek(0)
        
        # Upload the CSV data from the buffer to the S3 bucket
        file_name = date + '.csv'
        S3_CLIENT.put_object(Bucket=S3_PRICE_RAW_BUCKET, Key=file_name, Body=csv_buffer.getvalue())
    
    print(f"CSV file '{file_name}' uploaded to bucket '{S3_PRICE_RAW_BUCKET}' successfully.", flush=True)


if __name__ == "__main__":

    driver = get_selenium_driver()
    category_urls = get_category_url()
    print(category_urls, flush=True)

    product_prices = get_product_prices(driver=driver, category_urls=category_urls)
    print('Got ' + str(len(product_prices)) + ' product prices', flush=True)

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    upload_price_to_s3(product_prices, date=date)


    
# def upload_to_postgres(file_name:str):
#     with io.BytesIO() as f:
#         S3_CLIENT.download_fileobj(S3_PRICE_RAW_BUCKET, file_name, f)
#         f.seek(0)
#         product_prices = pickle.load(f)

#     supabase = SUPABASE_CLIENT

#     supabase.table(SINK_TABLE)\
#         .insert(product_prices)\
#         .execute()
    
#     return True