from util import *


def get_product_details():

    supabase = SUPABASE_CLIENT
    scraper = CLOUD_SCRAPER

    res = []

    query = """
select
    distinct concat('https://www.chemistwarehouse.com.au/buy/', data_id::varchar)::varchar as url
from price
where data_id not in (select data_id from product)
"""
    response = supabase.rpc('run_custom_query', {'query_text': query})\
        .execute()

    urls = [i['result'] for i in response.data]
    print(str(len(urls)) + ' urls to process', flush=True)

    count = 1

    for url in urls:
        response = scraper.get(url)
        content = response.text

        soup = BeautifulSoup(content, 'html.parser')
        product_info = {}

        data_id = url.split('/')[4]
        try:
            product_name = soup.find('div', class_='product-name').contents[1].contents[0].strip()
        except:
            product_name = None

        product_info['data_id'] = data_id
        product_info['product_name'] = product_name
        product_info['image_url'] = 'http://static.chemistwarehouse.com.au/ams/media/productimages/' + data_id + '/original.jpg'
        product_info['product_page_url'] = 'https://www.chemistwarehouse.com.au/buy/' + data_id

        sections = soup.findAll('section', {'class':'product-info-section'})
        filtered_sections = [section for section in sections if 'hidden' not in section.get('class', [])]

        for s in filtered_sections:
            section_name = s.get('class', [])[1].replace('-', '_')
            section_conent = s.find('div').contents
            section_conent_text = '\n'.join([i.text for i in section_conent]).strip()
            product_info[section_name] = section_conent_text

        res.append(product_info)

        if count % 10 == 0:
            print(str(count) + ' urls processed', flush=True)
        count += 1
    
    return res

def upload_product_details(product_details:dict):
    supabase = SUPABASE_CLIENT

    supabase.table('product')\
        .upsert(product_details)\
        .execute()

    return True

product_details = get_product_details()
print('finish processing product urls', flush=True)
if len(product_details) > 0:
    upload_product_details(product_details)
print('finish processing product descriptions', flush=True)