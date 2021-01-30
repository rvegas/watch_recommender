from google.cloud import storage
from scrapy.crawler import CrawlerProcess
from datetime import datetime

import scrapy
import json
import tempfile

TEMPORARY_FILE = tempfile.NamedTemporaryFile(delete=False, mode='w+t')

def upload_file_to_bucket(bucket_name, blob_file, destination_file_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob_source = bucket.blob('_source_' + destination_file_name)

    blob.upload_from_filename(blob_file.name, content_type='text/csv')
    blob_source.upload_from_filename(blob_file.name, content_type='text/csv')

class CreationSpider(scrapy.Spider):
    name = 'creationSpider'
    start_urls = ['https://www.creationwatches.com/products/seiko-75/', 
                  'https://www.creationwatches.com/products/orient-watches-252/', 
                  'https://www.creationwatches.com/products/citizen-74/',
                  'https://www.creationwatches.com/products/hamilton-watches-250/'
                  ]
    
    def parse(self, response):
        model_found = False
        for product in response.css('body#productinfo div.main-wrapper div.section'):
            if model_found:
                continue
            if product.css('div.pd-row p.para8 ::text').extract_first() != '':
                model_found = True
            model = product.css('div.pd-row p.para8 ::text').extract_first()
            name = product.css('div.pd-row h1.heading10 ::text').extract_first()
            price = product.css('div.pd-row p.product-price1 span.red ::text').extract_first()
            rating = product.css('div.pd-row div.rating span ::text').extract()
        
        comments_found = False
        for comments_section in response.css('body#productinfo div.main-wrapper'):
            if comments_found:
                continue
            comments = ''.join(comments_section.css('div.section4 div.section5 p ::text').extract()).replace(',', '').replace("\n", '').replace("\r", '')
            c = comments_section.css('div.section4 div.section5 p ::text').extract()
            if len(c) != 0:
                comments_found = True
        if model_found:    
            TEMPORARY_FILE.writelines("{},{},{},{},{}".format(model, name, price, rating, comments))

        for next_page in response.css('a[title=" Next Page "]'):
            yield response.follow(next_page, self.parse)
        for product_page in response.css('div.section h3.product-name a'):
            yield response.follow(product_page, self.parse)

process = CrawlerProcess({
    'USER_AGENT': 'GOOGLE SEO BOT'
})
process.crawl(CreationSpider)

def activate(request):
    now = datetime.now() 
    TEMPORARY_FILE.seek(0)
    request_json = request.get_json()
    BUCKET_NAME = 'bucket-cluster-ricardo'
    DESTINATION_FILE_NAME = 'input/crawlings/creation_watches.csv'
    
    process.start()
    TEMPORARY_FILE.seek(0)
    upload_file_to_bucket(BUCKET_NAME, TEMPORARY_FILE, DESTINATION_FILE_NAME)
    
    return "Success!"
