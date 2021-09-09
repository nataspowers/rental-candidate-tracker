# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

import boto3
from boto3.dynamodb.conditions import Key, Attr

from datetime import datetime

from decimal import Decimal
import json

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

class FurnishedFilterPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('furnished',False):
            raise DropItem(f"Furnished property: {item!r}")
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
        session = boto3.Session(region_name=self.region)
        credentials = session.get_credentials()
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(self.dynamodb_table)

        scan_kwargs = {
            'FilterExpression': Key('status').eq('active') | Key('status').eq('removed') | Key('status').eq('off-market'),
            'ProjectionExpression': "Address, #s",
            'ExpressionAttributeNames': {'#s':'status',}
        }
        done = False
        start_key = None
        items = []

        start = datetime.now()
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            items = items + response.get('Items', [])
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None

        print('Loaded {} candidate addresses in {}'.format(len(items), datetime.now() - start))
        self.previous_addresses = {a['Address']:a['status'] for a in items}


    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        candidate = {
            'Address': '{}, {}'.format(adapter['address'],adapter['city_state']),
            'source': spider.name,
            'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'active',
            'url': adapter['url'],
            'details': {
                'features': adapter.get('features',None),
                'pets': adapter['pets'],
                'bedrooms': adapter.get('bedrooms',None),
                'bathrooms': adapter.get('bathrooms',None),
                'description' : adapter.get('description',None),
                'telephone' : adapter.get('telephone',None),
                'year_built' : adapter.get('year_built',None),
                'property_type' : adapter.get('property_type',None),
                'days_on_market' : adapter.get('days_on_market',None),
                #'parking' : ''.join([str(elem) for elem in adapter.get('parking',[])]),
                #'floors' : adapter.get('floors',None),
                'heating' : adapter.get('heating',None),
                'ac' : adapter.get('ac',None),
                'fitness' : adapter.get('fitness',None),
                'attributes' : adapter.get('attributes',None),
                'furnished' : adapter.get('furnished',None)
            }
        }

        if 'price' in adapter:
            if '-' in adapter['price']:
                s = adapter['price'].split('-')
                if s[0].strip() == s[1].strip():
                    candidate['price'] = {'rent':int(s[0])}
                else:
                    candidate['price'] = {'rent':adapter['price']}
            else:
                candidate['price'] = {'rent':int(adapter['price'])}
        if 'area' in adapter:
            if '-' in adapter['area']:
                s = adapter['area'].split('-')
                if s[0].strip() == s[1].strip():
                    candidate['details']['area'] = int(s[0])
                else:
                    candidate['details']['area'] = adapter['area']
            else:
                candidate['details']['area'] = adapter['area']

        if 'deposit' in adapter:
            candidate['price']['deposit'] = adapter['deposit']
        if 'neighborhood' in adapter:
            candidate['neighborhood'] = {'name': adapter['neighborhood'],
                                         'url': adapter['neighborhood_url']}

         # remove empty strings and None
        candidate = {key:val for key, val in candidate.items() if val}
        candidate['details'] = {key:val for key, val in candidate['details'].items() if val}

        dynamodb = boto3.resource('dynamodb',region_name=self.region)
        table = dynamodb.Table(self.dynamodb_table)

        # TODO: if we want to handle multiple sources, will need a seperate list
        # If the address exists from another source, would need a semi-complicated merge
        if candidate['Address'] not in self.previous_addresses:
            print('  ***** New Property -> {}  *****'.format(candidate['Address']))
            table.put_item(Item=json.loads(json.dumps(candidate), parse_float=Decimal))

        else:
            #use the status we had from before, so that we don't "unremove" address that have been removed
            if self.previous_addresses[candidate['Address']] != 'off-market':
                candidate['status'] = self.previous_addresses[candidate['Address']]
            else:
                print('  ***** Changing Property from off-market to active -> {}  *****'.format(candidate['Address']))
                candidate['status'] = 'active'

            self.previous_addresses.pop(candidate['Address'])
            print('  ***** Existing Property -> {}, status {}  *****'.format(candidate['Address'], candidate['status']))

            update_exp = 'set #s1=:1, #s2=:2, #u1=:3, details=:4, refreshed=:5'
            expression_attr_values = {
                ':1': candidate['source'],
                ':2': candidate['status'],
                ':3': candidate['url'],
                ':4': json.loads(json.dumps(candidate['details']), parse_float=Decimal),
                ':5': candidate['created']
            }
            if 'price' in candidate:
                update_exp += ', price=:6'
                expression_attr_values[':6'] = json.loads(json.dumps(candidate['price']), parse_float=Decimal)
            if 'neighborhood' in candidate:
                update_exp += ', neighborhood=:7'
                expression_attr_values[':7'] = candidate['neighborhood']

            table.update_item(
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
        update = [address for address, status in self.previous_addresses.items() if status != 'off-market']
        print('Closing Spider - marking {} addresses as off-market'.format(len(update)))

        session = boto3.Session(region_name=self.region)
        credentials = session.get_credentials()
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(self.dynamodb_table)


        for address in update:
            print('  ***** Off-Market -> {}  *****'.format(address))
            table.update_item(
                Key={
                        'Address': address,
                    },
                UpdateExpression="set #s = :a, closed = :b",
                ExpressionAttributeNames={
                    '#s':'status'
                },
                ExpressionAttributeValues={
                       ':a': "off-market",
                        ':b': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
            )
