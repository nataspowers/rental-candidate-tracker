import sys
import json

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def lambda_handler(event, context):
    process = CrawlerProcess(get_project_settings())
    process.crawl('trulia')
    process.start() # the script will block here until the crawling is finished