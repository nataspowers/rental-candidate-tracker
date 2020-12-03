# -*- coding: utf-8 -*-
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Identity, Compose, Join
import scrapy
from trulia_scraper.parsing import remove_empty, get_number_from_string


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

    # Items generated from further parsing of 'raw' scraped data
    section8 = scrapy.Field()
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

    price_out = Compose(TakeFirst(), lambda s: int(s.replace(',', '')))
    description_out = Compose(remove_empty)
    features_out = Compose(remove_empty)
    heating_out = Compose(remove_empty)
    floors_out = Compose(remove_empty)
    city_state_out = Join(', ')

    attribute_values_out = Compose(remove_empty)
    attribute_names_out = Compose(remove_empty)

    area_out = Compose(TakeFirst(), get_number_from_string)
    bedrooms_out = Compose(TakeFirst(), int)
    bathrooms_out = Compose(TakeFirst(), float)
    deposit = Compose(TakeFirst(), lambda s: int(s.replace(',', '')))
    year_built = Compose(TakeFirst(), int)
    days_on_market = Compose(TakeFirst(), int)
    year_built = Compose(TakeFirst(), int)