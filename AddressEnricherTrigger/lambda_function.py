import json
import boto3
from decimal import Decimal
from datetime import datetime, timedelta

if __name__ == '__main__':
    """
        Before we import other modules - if we are running as a script we need to load the
        environment variables from template.yaml
    """
    import yaml
    import os
    with open('template.yaml', 'r') as stream:
        var = yaml.load(stream, Loader=yaml.FullLoader)
        for k,v in var['Resources']['AddressEnricherTrigger']['Properties']['Environment']['Variables'].items():
            os.environ[k] = v

from crime import get_crime, load_oakland_crime
from util import get_distance, get_next_weekday
from locations import *
from commute import *
from walkscore import get_walk_score


print('Loading function')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    sources = []
    for record in event['Records']:
        address = None
        #print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
        if record['eventName'] == 'INSERT':
            address = record['dynamodb']['Keys']['Address']['S']

        if record['eventName'] == 'MODIFY':
            if not 'commute' in record['dynamodb']['NewImage']:
                if not record['dynamodb']['NewImage']['status']['S'] == 'off-market':
                    address = record['dynamodb']['NewImage']['Address']['S']

        if address != None:
            address_detail = geocode(address=address, api="google")
            #print(json.dumps(address_detail, indent=2))
            sources.append({
                'original_address':address,
                'formatted_address':address_detail['formatted_address'],
                'geo':address_detail['geo']
            })
            print('Input "{}" Geocoded "{}"'.format(address, address_detail['formatted_address']))

    if sources:
        formatted_addresses = [item['formatted_address'] for item in sources]
        geos = [item['geo'] for item in sources]
        drive_times = get_drive_time_friend(formatted_addresses)
        #print('Drive to Friend {}'.format(json.dumps(drive_times, indent=2)))

        transit_commute = get_commute_transit(formatted_addresses)
        #print('Commute Transit {}'.format(json.dumps(transit_commute, indent=2)))
        drive_commute = get_commute_drive(formatted_addresses)
        #print('Commute Drive {}'.format(json.dumps(drive_commute, indent=2)))

        airport_transit_commute = get_airport_commute_transit(formatted_addresses)
        #print('Airport Commute Transit {}'.format(json.dumps(airport_transit_commute, indent=2)))
        airport_drive_commute = get_airport_commute_drive(formatted_addresses)
        #print('Airport Commute Drive {}'.format(json.dumps(airport_drive_commute, indent=2)))


    for address in sources:

        formatted_address = address['formatted_address']
        geo = address['geo']

        scores = get_walk_score(geo, formatted_address)
        #print('Walk Scores {}'.format(json.dumps(scores, indent=2)))

        crime = get_crime(formatted_address, geo, 'mixed')
        #crime = {}
        print('Soda crime score for {} - {}'.format(formatted_address, json.dumps(crime, indent=2)))

        #crime = get_crime(formatted_address, geo, 'crimeometer')
        #crime = {}
        #print('Crimeometer crime score for {} - {}'.format(formatted_address, json.dumps(crime, indent=2)))

        coffee = get_coffee_shops(geo)
        #print('Coffee {}'.format(json.dumps(coffee, indent=2)))

        restaurant = get_restaurants(geo)
        #print('Restaurants {}'.format(json.dumps(restaurant, indent=2)))

        stores = get_convenience_store(geo)
        #print('Convenience Stores {}'.format(json.dumps(stores, indent=2)))

        bart = get_bart(geo)
        #print('Bart {}'.format(json.dumps(bart, indent=2)))
        bart_geo = (bart['place']['geometry']['location']['lat'],
                        bart['place']['geometry']['location']['lng'])
        bart_commute = get_walking_time(geo, bart_geo)
        #print('Bart Commute {}'.format(json.dumps(bart_commute, indent=2)))
        bart['commute'] = {
            'distance' : bart_commute['rows'][0]['elements'][0]['distance'],
            'duration' : bart_commute['rows'][0]['elements'][0]['duration']
        }

        friend_commute = fetch_drive_time(formatted_address, drive_times, formatted_addresses)[0]

        work =     {'transit' : fetch_drive_time(formatted_address, transit_commute, formatted_addresses)[0],
                    'drive' : fetch_drive_time(formatted_address, drive_commute, formatted_addresses)[0]}
        airports = {'transit' : get_airport_commute(formatted_address, airport_transit_commute, formatted_addresses),
                    'drive' : get_airport_commute(formatted_address, airport_drive_commute, formatted_addresses)}
        #print('Airport Commute {}'.format(json.dumps(airports, indent=2)))

        commute = {
            'work' : work,
            'friend' : friend_commute,
            'airports' : airports
        }

        places = {
            'coffee' : coffee,
            'restaurant' : restaurant,
            'convenience_store' : stores,
            'bart' : bart
        }
        """
        print('Cadidate {} - commute = {}, places = {}, walk_score = {}, crime = {}'
            .format(address,
                    json.dumps(commute, indent=2),
                    json.dumps(places, indent=2),
                    json.dumps(scores, indent=2),
                    json.dumps(crime, indent=2)
                    )
            )
        """
        res = update_table(address, commute, places, scores, crime)

    return 'Successfully processed {} records.'.format(len(event['Records']))

def get_airport_commute(address, commute, index_list):
    commutes = {}
    for idx, airport in enumerate(commute['destination_addresses']):
        commutes[airport] = fetch_drive_time(address, commute, index_list)[idx]
    return commutes

def update_table (address, commute, places, score, crime):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    location = json.loads(json.dumps({
        'address': address['formatted_address'],
        'geo': address['geo']
    }),parse_float=Decimal)
    commute = json.loads(json.dumps(commute), parse_float=Decimal)
    places = json.loads(json.dumps(places), parse_float=Decimal)
    score = json.loads(json.dumps(score), parse_float=Decimal)
    crime = json.loads(json.dumps(crime), parse_float=Decimal)

    #print('updating table {} - commute = {}, places = {}, walk_score = {}, crime = {}'
    #        .format(key, commute, places, score, crime))
    update_expr = 'set #s1 = :val1, commute = :val2, places = :val3, walk_score = :val4'
    expression_attr_values = {
        ':val1': location,
        ':val2': commute,
        ':val3': places,
        ':val4': score,
    }
    if crime:
        update_expr += ', crime = :val5'
        expression_attr_values[':val5'] = crime

    response = table.update_item(
                Key={'Address': address['original_address']},
                UpdateExpression=update_expr,
                ExpressionAttributeNames = {
                  '#s1':'location'
                },
                ExpressionAttributeValues=expression_attr_values
           )
    return response
    #return ''

def load_all_candidates (filter = True):

    from boto3.dynamodb.conditions import Key

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    scan_kwargs = {
        'FilterExpression': Key('status').eq('active'),
        'ProjectionExpression': "Address, commute"
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

    candidates = []
    for item in items:
        #print(item)
        if not filter or (filter and not 'commute' in item):
            candidates.append({
                'eventName':'INSERT',
                'dynamodb': {
                    'Keys': {
                        'Address': {
                            'S': item['Address']
                        }
                    }
                }
            })
    print('Loaded {} candidate addresses (of {}) in {}'.format(len(candidates), len(items), datetime.now() - start))
    return { 'Records': candidates }

if __name__ == '__main__':
    import os
    #load_oakland_crime(api='bing', config={'api_key':os.environ['bing_key']})
    load_oakland_crime()
    candidates = load_all_candidates(filter=True)
    lambda_handler(candidates,None)