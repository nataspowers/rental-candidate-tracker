# -*- coding: utf-8 -*-
import os
import scrapy
import math
import datetime
import urllib.parse
from scrapy.linkextractors import LinkExtractor
from trulia_scraper.items import TruliaItem, TruliaItemLoader
from trulia_scraper.parsing import get_number_from_string
from scrapy.utils.conf import closest_scrapy_cfg
from scraper_api import ScraperAPIClient


class TruliaSpider(scrapy.Spider):
    client = ScraperAPIClient('22c786c81d0f6a84eb1312a0d6c6aec5')
    name = 'trulia'
    allowed_domains = ['trulia.com']
    custom_settings = {'FEED_URI': '/tmp/data/data_for_rent_%(time)s.jl',
                       'FEED_FORMAT': 'jsonlines'}

    def __init__(self, state='CA', cities=['Oakland','Alameda','Berkeley','Emeryville'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = state
        self.cities = cities
        url = 'https://trulia.com/for_rent/{city},{state}/2000-4000_price/1300p_sqft/'
        self.base_urls = [url.format(state=state, city=city) for city in self.cities]
        self.start_urls = [self.client.scrapyGet(url=url) for url in self.base_urls]
        self.le = LinkExtractor(restrict_xpaths='//*[@id="resultsColumn"]/div[1]')

    def parse(self, response):
        N = self.get_number_of_pages_to_scrape(response)
        self.logger.info("Determined that property pages are contained on {N} different index pages, each containing at most 30 properties. Proceeding to scrape each index page...".format(N=N))
        self.logger.info("Response URL is {}".format(response.url))
        for base_url in self.base_urls:
            for url in [base_url + "{n}_p/".format(n=n) for n in range(1, N+1)]:
                yield scrapy.Request(url=self.client.scrapyGet(url), callback=self.parse_index_page, dont_filter=True)

    @staticmethod
    def get_number_of_pages_to_scrape(response):
        pagination = response.xpath('//*[@data-testid="pagination-caption"]/text()')
        if pagination.re(r'1-30 of ([\d,]+) Results'):
            number_of_results = int(pagination.re(r'1-30 of ([\d,]+) Results')[0])
            return math.ceil(number_of_results/30)
        else:
            return 1

    def parse_index_page(self, response):
        links = self.le.extract_links(response)
        for link in links:
            link.url = link.url.replace('https://api.scraperapi.com','https://trulia.com')
            yield scrapy.Request(url=self.client.scrapyGet(link.url), callback=self.parse_property_page, dont_filter=True)

    def parse_property_page(self, response):
        l = TruliaItemLoader(item=TruliaItem(), response=response)
        self.load_common_fields(item_loader=l, response=response)

        item = l.load_item()
        self.post_process(item=item)
        return item

    @staticmethod
    def load_common_fields(item_loader, response):
        '''Load field values which are common to "on sale" and "recently sold" properties.'''
        item_loader.add_value('url', urllib.parse.unquote(response.url[32:-60]))
        item_loader.add_xpath('address', '//*[@data-testid="home-details-summary-headline"]/text()')
        item_loader.add_xpath('city_state', '//*[@data-testid="home-details-summary-city-state"]/text()')
        item_loader.add_xpath('price', '//*[@data-testid="on-market-price-details"]/div/text()', re=r'\$([\d,]+)')
        item_loader.add_xpath('neighborhood', '//*[@data-testid="neighborhood-link"]/text()')
        item_loader.add_xpath('neighborhood_url', '//*[@data-testid="neighborhood-link"]/@href')

        fact_list = item_loader.nested_xpath('//*[@data-testid="facts-list"]')
        fact_list.add_xpath('bedrooms', xpath='.//*[@data-testid="bed"]/div/div[2]/text()', re=r'(\d+) (?:Beds|Bed|beds|bed)$')
        fact_list.add_xpath('bathrooms', xpath='.//*[@data-testid="bath"]/div/div[2]/text()', re=r'(\d+\.?[0-9]*) (?:Baths|Bath|baths|bath)$')
        fact_list.add_xpath('area', xpath='.//*[@data-testid="floor"]/div/div[2]/text()', re=r'([\d, -]+)')

        item_loader.add_xpath('telephone', '//*[@data-testid="home-description-text-description-phone-number"]/div/div[2]/text()')
        item_loader.add_xpath('description', '//*[@data-testid="home-description-text-description-text"]/text()')

        item_loader.add_xpath('tags','//*[@data-testid="hero-image-property-tag-1"]/span/text()')
        item_loader.add_xpath('tags','//*[@data-testid="hero-image-property-tag-2"]/span/text()')
        item_loader.add_xpath('tags','//*[@data-testid="hero-image-property-tag-3"]/span/text()')
        item_loader.add_xpath('tags','//*[@data-testid="hero-image-property-tag-4"]/span/text()')
        item_loader.add_xpath('tags','//*[@data-testid="hero-image-property-tag-5"]/span/text()')

        features = item_loader.nested_xpath('//*[@data-testid="structured-amenities-table-category"]')
        #features without a sub-header are 1 div below /table/tbody/tr/td
        features.add_xpath('features', xpath='.//table/tbody/tr/td/div/li/text()')
        #features with a sub-header are 2 divs below /table/tbody/tr/td
        #should we try to get the sub-header and associate it to the value? it would be in .//table/tbody/tr/td/div/div/text()
        features.add_xpath('features', xpath='.//table/tbody/tr/td/div[2]/li/text()')

        item_loader.add_xpath('attribute_values', '//*[@data-testid="wls-attribute"]/div/div/div/div/div/div[2]/div/text()', re=r'\d+')
        item_loader.add_xpath('attribute_names', '//*[@data-testid="wls-attribute"]/div/div/div[2]/div/div[2]/div/text()')



    @staticmethod
    def post_process(item):
        '''Add any additional data to an item after loading it'''
        s8 = ['section 8', 'section8', 'GoSection8.com']
        np = ['no pet', 'no pets', 'pets not allowed', 'no pets allowed','pets are not allowed']
        p = ['dog', 'cat', 'cats','small dogs', 'pet considered','pets considered','pets allowed','pets ok','pets okay','pets negotiable']

        item['features'] = list(dict.fromkeys(item.get('features',[]))) #remove list duplicates
        features = [str.casefold(f) for f in item.get('features',[])]
        description = [str.casefold(d) for d in item.get('description',[])]
        f_d = features + description
        tags = item.get('tags',[])

        section8 = [item for s8_phrase in s8 for item in f_d if s8_phrase in item]
        item['section8'] = False if not section8 else True

        if 'INCOME RESTRICTED' in tags or 'SENIOR HOUSING' in tags:
            item['section8'] = True

        no_pets = [item for np_phrase in np for item in f_d if np_phrase in item]
        yes_pets = [item for p_phrase in p for item in f_d if p_phrase in item]

        if no_pets:
            item['pets'] = False
        elif yes_pets:
            item['pets'] = True
        else:
            item['pets'] = None

        if 'PET FRIENDLY' in tags:
            item['pets'] = True

        if 'FURNISHED' in tags:
            item['furnished'] = True

        deposit = ' '.join([i for i in f_d if 'deposit' in i])
        deposit = [i for i in deposit.split() if i.replace('$','').replace(',','').isdigit()]
        if deposit:
            item['deposit'] = int(deposit[0].replace('$','').replace(',',''))

        year_built = ' '.join([i for i in f_d if 'year built' in i])
        year_built = [i for i in year_built.split() if i.isdigit()]
        if year_built:
            item['year_built'] = int(year_built[0])

        property_type = ' '.join([i for i in f_d if 'property type' in i])
        if property_type:
            item['property_type'] = property_type.replace('property type: ','')

        days_on_market = ' '.join([i for i in f_d if 'days on market' in i])
        days_on_market = [i for i in days_on_market.split() if i.isdigit()]
        if days_on_market:
            item['days_on_market'] = int(days_on_market[0])

        parking = ' '.join([i for i in f_d if 'parking' in i])
        if parking:
            item['parking'] = parking.replace('parking: ','')

        floors = ' '.join([i for i in f_d if 'floors' in i])
        if floors:
            item['floors'] = floors.replace('floors: ','')

        heating = ' '.join([i for i in f_d if 'heating' in i])
        if heating:
            item['heating'] = heating.replace('heating: ','')

        ac = [i for i in f_d if 'air conditioning' in i]
        if ac:
            item['ac'] = True

        fitness = [i for i in f_d if 'fitness center' in i]
        if fitness:
            item['fitness'] = True

        if 'attribute_names' in item and 'attribute_values' in item:
            item['attributes'] = {}
            for idx, val in enumerate(item['attribute_names']):
                item['attributes'][val] = item['attribute_values'][idx]

            del item['attribute_names']
            del item['attribute_values']