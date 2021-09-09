# -*- coding: utf-8 -*-
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Identity, Compose, Join
import scrapy
from trulia_scraper.parsing import remove_empty, get_number_from_string, take_first_two


class TruliaItem(scrapy.Item):
    url = scrapy.Field()
    address = scrapy.Field()
    city_state = scrapy.Field()
    price = scrapy.Field()              # for items on sale only
    neighborhood = scrapy.Field()
    neighborhood_url = scrapy.Field()
    description = scrapy.Field()
    features = scrapy.Field()
    area = scrapy.Field()
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    attribute_names = scrapy.Field()
    attribute_values = scrapy.Field()
    telephone = scrapy.Field()
    tags = scrapy.Field()

    # Items generated from further parsing of 'raw' scraped data
    section8 = scrapy.Field()
    furnished = scrapy.Field()
    pets = scrapy.Field()
    deposit = scrapy.Field()
    year_built = scrapy.Field()
    property_type = scrapy.Field()
    days_on_market = scrapy.Field()
    parking = scrapy.Field()
    floors = scrapy.Field()
    heating = scrapy.Field()
    fitness = scrapy.Field()
    ac = scrapy.Field()
    attributes = scrapy.Field()


class TruliaItemLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    price_out = Compose(lambda v: take_first_two(v), Join(' - '), lambda s: s.replace(',','')) # we will get multiple items if the price is a range - join to gether to make a string "<price 1> - <price 2>"
    description_out = Compose(remove_empty)
    features_out = Compose(remove_empty)
    heating_out = Compose(remove_empty)
    floors_out = Compose(remove_empty)
    city_state_out = Join(', ')
    tags_out = Compose(remove_empty)

    attribute_values_out = Compose(remove_empty)
    attribute_names_out = Compose(remove_empty)

    area_out = Compose(TakeFirst(), lambda s: s.replace(',', ''), str.strip) # area could be "2,500" or a range, "2,500 - 5,000". To keep range we do not convert to int
    bedrooms_out = Compose(TakeFirst(), int)
    bathrooms_out = Compose(TakeFirst(), float)
    deposit = Compose(TakeFirst(), lambda s: int(s.replace(',', '')))
    year_built = Compose(TakeFirst(), int)
    days_on_market = Compose(TakeFirst(), int)
    year_built = Compose(TakeFirst(), int)