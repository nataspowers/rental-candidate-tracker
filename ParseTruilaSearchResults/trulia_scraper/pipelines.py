# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

import boto3
from boto3.dynamodb.conditions import Key

from datetime import datetime

from decimal import *

class DuplicatesPipeline:
    def __init__(self):
        self.addresses_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        address = '{}, {}'.format(adapter['address'],adapter['city_state'])
        if address in self.addresses_seen:
            raise DropItem(f"Duplicate address found: {item!r}")
        else:
            self.addresses_seen.add(address)
            return item

class Section8FilterPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter['section8']:
            raise DropItem(f"Section 8 property: {item!r}")
        else:
            return item

class NoPetFilterPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter['pets'] == False:
            raise DropItem(f"No Pet property: {item!r}")
        else:
            return item


class DynamoDBPipeline:
    def __init__(self,dynamodb_table,region):
        self.dynamodb_table = dynamodb_table
        self.region = region

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            dynamodb_table=crawler.settings.get('DYNAMODB_TABLE'),
            region=crawler.settings.get('REGION')
        )

    def open_spider(self,spider):
        self.dynamodb = boto3.resource('dynamodb',region_name=self.region)
        self.table = self.dynamodb.Table(self.dynamodb_table)

        scan_kwargs = {
            'FilterExpression': Key('status').eq('active'),
            'ProjectionExpression': "Address"
        }
        done = False
        start_key = None
        items = []

        start = datetime.now()
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = self.table.scan(**scan_kwargs)
            items = items + response.get('Items', [])
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None

        print('Loaded {} candidate addresses in {}'.format(len(items), datetime.now() - start))
        self.previous_addresses = [a['Address'] for a in items]


    def process_item(self, item, spider):

        candidate = {
            'Address': '{}, {}'.format(item['address'],item['city_state']),
            'source': spider.name,
            'status': 'active',
            'url': item['url'],
            'details': {
                'features': item.get('features',None),
                'pets': item['pets'],
            }
        }
        if 'telephone' in item:
            candidate['details']['telephone'] = item['telephone']
        if 'description' in item:
            candidate['details']['description'] = item['description']
        if 'bedrooms' in item:
            candidate['details']['bedrooms'] = item['bedrooms']
        if 'bathrooms' in item:
            candidate['details']['bathrooms'] = Decimal(str(item['bathrooms']))
        if 'price' in item:
            if '-' in item['price']:
                s = item['price'].split('-')
                if s[0].strip() == s[1].strip():
                    candidate['price'] = {'rent':int(s[0])}
                else:
                    candidate['price'] = {'rent':item['price']}
            else:
                candidate['price'] = {'rent':int(item['price'])}
        if 'area' in item:
            if '-' in item['area']:
                s = item['area'].split('-')
                if s[0].strip() == s[1].strip():
                    candidate['details'] = {'area':int(s[0])}
                else:
                    candidate['details'] = {'area':item['area']}
            else:
                candidate['details'] = {'area':int(item['area'])}

        if 'deposit' in item:
            candidate['price']['deposit'] = item['deposit']
        if 'neighborhood' in item:
            candidate['neighborhood'] = {'name': item['neighborhood'],
                                         'url': item['neighborhood_url']}
        if 'year_built' in item:
            candidate['details']['year_built'] = item['year_built']
        if 'property_type' in item:
            candidate['details']['property_type'] = item['property_type']
        if 'days_on_market' in item:
            candidate['details']['days_on_market'] = item['days_on_market']
        if 'parking' in item:
            candidate['details']['parking'] = item['parking']
        if 'floors' in item:
            candidate['details']['floors'] = item['floors']
        if 'heating' in item:
            candidate['details']['heating'] = item['heating']
        if 'ac' in item:
            candidate['details']['ac'] = item['ac']
        if 'fitness' in item:
            candidate['details']['fitness'] = item['fitness']
        if 'attributes' in item:
            candidate['details']['attributes'] = item['attributes']

        # TODO: if we want to handle multiple sources, will need a seperate list
        # If the address exists from another source, would need a semi-complicated merge
        if candidate['Address'] not in self.previous_addresses:

            self.table.put_item(Item=candidate)

        else:
            self.previous_addresses.remove(candidate['Address'])
            update_exp = 'set #s1=:1, #s2=:2, #u1=:3, details=:4'
            expression_attr_values = {
                ':1': candidate['source'],
                ':2': candidate['status'],
                ':3': candidate['url'],
                ':4': candidate['details']
            }
            if 'price' in candidate:
                update_exp += ', price=:5'
                expression_attr_values[':5'] = candidate['price']
            if 'neighborhood' in candidate:
                update_exp += ', neighborhood=:6'
                expression_attr_values[':6'] = candidate['neighborhood']

            self.table.update_item(
                Key={
                    'Address': candidate['Address']
                },
                UpdateExpression=update_exp,
                ExpressionAttributeNames={
                    '#s1':'source',
                    '#s2':'status',
                    '#u1':'url'
                },
                ExpressionAttributeValues=expression_attr_values
            )

        return item

    def close_spider(self,spider):
        for address in self.previous_addresses:
            self.table.update_item(
                Key={
                        'Address': address,
                    },
                UpdateExpression="set #s = :a",
                ExpressionAttributeNames={
                    '#s':'status'
                },
                ExpressionAttributeValues={
                        ':a': "off-market"
                    }
            )
