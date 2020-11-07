# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

import boto3
from boto3.dynamodb.conditions import Key

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
            region=crawler.setting.get('REGION')
        )

    def open_spider(self,spider):
        self.dynamodb = boto3.resource('dynamodb',region_name=self.region)
        self.table = self.dynamodb.Table(self.dynamodb_table)
        
        resp = self.table.query(
                    IndexName="status-source-index",
                    KeyConditionExpression=Key('status').eq('active') & Key('source').eq(spider.name)
                    )
        self.previous_addresses = [i['Address'] for i in resp['Items']]

    def process_item(self, item, spider):

        candidate = {
            'Address': '{}, {}'.format(item['address'],item['city_state']),
            'source': spider.name,
            'status': 'active',
            'url': item['url'],
            'price': {
                'rent': item['price'],
                'deposit': item['deposit']
            },
            'neighborhood': {
                'name': item['neighborhood'],
                'url': item['neighborhood_url']
            },
            'details':{
                'description': item['description'],
                'features': item['features'],
                'area': item['area'],
                'bedrooms': item['bedrooms'],
                'bathrooms': item['bathrooms'],
                'pets': item['pets'],
                'year_built': item['year_built'],
                'property_type': item['property_type'],
                'days_on_market': item['days_on_market'],
                'parking': item['parking'],
                'floors': item['floors'],
                'heating': item['heating']
            }

        }

        if candidate['Address'] not in self.previous_addresses:

            self.table.put_item(
                Item={candidate}
            )
            
        else:
            
            self.previous_addresses.remove(candidate['Address'])
            self.table.update_item(
                Key={
                    'Address': candidate['Address']
                },
                UpdateExpression="set #s1=:1, #s2=:2, #u1=:3, price=:4, neighborhood=:5, details=:6",
                ExpressionAttributeNames={
                    '#s1':'source',
                    '#s2':'status',
                    '#u1':'url'
                },
                ExpressionAttributeValues={
                            ':1': candidate['source'],
                            ':2': candidate['status'],
                            ':3': candidate['url'],
                            ':4': candidate['price'],
                            ':5': candidate['neighborhood'],
                            ':6': candidate['details']
                }
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
