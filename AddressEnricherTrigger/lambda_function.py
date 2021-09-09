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

def score_records(event, types):
    #print("Received event: " + json.dumps(event, indent=2))
    if types == 'all':
        types = ['work','airport','friend','restaurant','coffee','crime', 'walk_score', 'convenience_store', 'bart']

    print('scoring types: {}'.format(types))

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
            #print(json.dumps(record, indent=2, cls=DecimalEncoder))
            if 'location' not in record['dynamodb']['Keys']:
                address_detail = geocode(address=address, api="google")
                #print(json.dumps(address_detail, indent=2))
                print('Input "{}" Geocoded "{}"'.format(address, address_detail['formatted_address']))
            else:
                address_detail = {
                    'formatted_address' : record['dynamodb']['Keys']['location']['M']['address']['S'],
                    'geo' : [float(i['N']) for i in record['dynamodb']['Keys']['location']['M']['geo']['L']]
                }
                print('Loaded address "{}" and formatted address "{}"'.format(address, address_detail['formatted_address']))

            sources.append({
                'original_address':address,
                'formatted_address':address_detail['formatted_address'],
                'geo':address_detail['geo']
            })


    if sources and ('work' in types or 'friend' in types or 'airport' in types):
        formatted_addresses = [item['formatted_address'] for item in sources]
        geos = [item['geo'] for item in sources]
        if 'friend' in types:
            drive_times = get_drive_time_friend(formatted_addresses)
            #print('Drive to Friend {}'.format(json.dumps(drive_times, indent=2)))

        if 'work' in types:
            transit_commute = get_commute_transit(formatted_addresses)
            #print('Commute Transit {}'.format(json.dumps(transit_commute, indent=2)))
            drive_commute = get_commute_drive(formatted_addresses)
            #print('Commute Drive {}'.format(json.dumps(drive_commute, indent=2)))

        if 'airport' in types:
            airport_transit_commute = get_airport_commute_transit(formatted_addresses)
            #print('Airport Commute Transit {}'.format(json.dumps(airport_transit_commute, indent=2)))
            airport_drive_commute = get_airport_commute_drive(formatted_addresses)
            #print('Airport Commute Drive {}'.format(json.dumps(airport_drive_commute, indent=2)))


    for address in sources:

        formatted_address = address['formatted_address']
        geo = address['geo']
        places = {}
        commute = {}
        scores = None
        crime = None

        if 'crime' in types:
            crime = get_crime(formatted_address, geo, 'mixed')
            #crime = {}
            print('Soda crime score for {} - {}'.format(formatted_address, json.dumps(crime, indent=2)))

            #crime = get_crime(formatted_address, geo, 'crimeometer')
            #crime = {}
            #print('Crimeometer crime score for {} - {}'.format(formatted_address, json.dumps(crime, indent=2)))

        if 'walk_score' in types:

            scores = get_walk_score(geo, formatted_address)
            #print('Walk Scores {}'.format(json.dumps(scores, indent=2)))

        if 'coffee' in types:
            places['coffee'] = get_coffee_shops(geo)
            #print('Coffee {}'.format(json.dumps(coffee, indent=2)))

        if 'restaurant' in types:
            places['restaurant'] = get_restaurants(geo)
            #print('Restaurants {}'.format(json.dumps(restaurant, indent=2)))

        if 'convenience_store' in types:
            places['convenience_store'] = get_convenience_store(geo)
            #print('Convenience Stores {}'.format(json.dumps(stores, indent=2)))

        if 'bart' in types:
            bart = get_bart(geo)
            #print('Bart {}'.format(json.dumps(bart, indent=2)))
            bart_geo = [bart['place']['geometry']['location']['lat'],
                        bart['place']['geometry']['location']['lng']]
            bart_commute = get_walking_time([geo], [bart_geo])
            #print('Bart Commute {}'.format(json.dumps(bart_commute, indent=2)))
            bart['commute'] = {
                'distance' : bart_commute['rows'][0]['elements'][0]['distance'],
                'duration' : bart_commute['rows'][0]['elements'][0]['duration']
            }
            places['bart'] = bart


        if 'friend' in types:
            commute['friend'] = fetch_drive_time(formatted_address, drive_times, formatted_addresses)[0]


        if 'work' in types:
            commute['work'] = {'transit' : fetch_drive_time(formatted_address, transit_commute, formatted_addresses)[0],
                                'drive' : fetch_drive_time(formatted_address, drive_commute, formatted_addresses)[0]}

        if 'airport' in types:
            commute['airports'] = {'transit' : get_airport_commute(formatted_address, airport_transit_commute, formatted_addresses),
                                    'drive' : get_airport_commute(formatted_address, airport_drive_commute, formatted_addresses)}
            #print('Airport Commute {}'.format(json.dumps(airports, indent=2)))



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
    ## This has a problem - we only want to update the part of the structure that we just recalculated
    ## so if we only did coffee, we don't want to update places.restaurant - as it could already exist.
    ## however, if places doesn't exist, and we try to update places.coffee we get an error.
    ## we could break this up into 4 seperate updates (as trying to figure out which piece is missing a parent when there are two of them is hard), and then do a try/catch to ensure that the parent map object exists
    ## or we could fetch the existing document first, and fill in any missing data from the existing object, and then update the entire object
    ## For now - we are going to do everything first before updating only targeted items, so we are sure that there are no missing parent maps
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    location = json.loads(json.dumps({
        'address': address['formatted_address'],
        'geo': address['geo']
    }), parse_float=Decimal)
    commute = json.loads(json.dumps(commute), parse_float=Decimal)
    places = json.loads(json.dumps(places), parse_float=Decimal)
    score = json.loads(json.dumps(score), parse_float=Decimal)
    crime = json.loads(json.dumps(crime), parse_float=Decimal)

    #print('updating table {} - commute = {}, places = {}, walk_score = {}, crime = {}'
    #        .format(key, commute, places, score, crime))
    update_expr = 'set #s1 = :val1, updated = :dt'
    expression_attr_values = {
        ':val1': location,
        ':dt' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    if commute:
        if commute.get('friend',False) and commute.get('work',False) and commute.get('airports',False):
            update_expr += ', commute = :val2'
            expression_attr_values[':val2'] = commute
        else:
            if commute.get('friend',False):
                update_expr += ', commute.friend = :val2a'
                expression_attr_values[':val2a'] = commute['friend']
            if commute.get('work',False):
                update_expr += ', commute.work = :val2b'
                expression_attr_values[':val2b'] = commute['work']
            if commute.get('airports',False):
                update_expr += ', commute.airports = :val2c'
                expression_attr_values[':val2c'] = commute['airports']
    if places:
        if places.get('coffee',False) and places.get('restaurant',False) and places.get('convenience_store',False) and places.get('bart',False):
            update_expr += ', places = :val3'
            expression_attr_values[':val3'] = places
        else:
            if places.get('coffee',False):
                update_expr += ', places.coffee = :val3a'
                expression_attr_values[':val3a'] = places['coffee']
            if places.get('restaurant',False):
                update_expr += ', places.restaurant = :val3b'
                expression_attr_values[':val3b'] = places['restaurant']
            if places.get('convenience_store',False):
                update_expr += ', places.convenience_store = :val3c'
                expression_attr_values[':val3c'] = places['convenience_store']
            if places.get('bart',False):
                update_expr += ', places.bart = :val3d'
                expression_attr_values[':val3d'] = places['bart']
    if score:
        update_expr += ', walk_score = :val4'
        expression_attr_values[':val4'] = score
    if crime:
        update_expr += ', crime = :val5'
        expression_attr_values[':val5'] = crime

    print('updating table - update expression = {}'.format(update_expr))


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

def load_all_candidates (filter = True, city = 'all', limit = -1, missing = []):

    from boto3.dynamodb.conditions import Key

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    scan_kwargs = {
        'FilterExpression': Key('status').eq('active'),
        'ProjectionExpression': "Address, #s1",
        'ExpressionAttributeNames' : {'#s1':'location'},
    }

    if filter:
        scan_kwargs['ProjectionExpression'] = scan_kwargs['ProjectionExpression'] + ', commute'


        if missing and any(item in ['places', 'coffee', 'restaurant', 'bart', 'convenience_store'] for item in missing):
            scan_kwargs['ProjectionExpression'] = scan_kwargs['ProjectionExpression'] + ', places'

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
        if (not filter or \
            (not item.get('commute',False)) or \
            (missing and
                ('places' in missing and not item.get('places',False)) or \
                ('coffee' in missing and not item.get('places',{}).get('coffee',False)) or \
                ('restaurant' in missing and not item.get('places',{}).get('restaurant',False)) or \
                ('bart' in missing and not item.get('places',{}).get('bart',False))  or \
                ('convenience_store' in missing and not item.get('places',{}).get('convenience_store',False)) or \
                ('crime' in missing and not item.get('crime',{}))
                )

            ):
            if city == 'all' or item['Address'].lower().find(city.lower()) != -1:
                candidate = {
                    'eventName':'INSERT',
                    'dynamodb': {
                        'Keys': {
                            'Address': {
                                'S': item['Address']
                            }
                        }
                    }
                }
                if 'location' in item:
                    candidate['dynamodb']['Keys']['location'] = {
                        "M": {
                            'geo' : {
                                'L' : [{'N':i} for i in item['location']['geo']]
                            },
                            'address' : {
                                'S': item['location']['address']
                            }
                        }
                      }
                candidates.append(candidate)
    if limit == -1:
        limit = len(candidates)
    print('Loaded {} candidate addresses (of {}) in {} - limiting to {}'.format(len(candidates), len(items), datetime.now() - start, len(candidates[:limit])))
    return { 'Records': candidates[:limit] }

def lambda_handler(event, context):
    load_oakland_crime()
    score_records(event, 'all')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

if __name__ == '__main__':
    import os
    #load_oakland_crime(api='bing', config={'api_key':os.environ['bing_key']})
    load_oakland_crime()
    #candidates = load_all_candidates(filter=False, city='all', limit = -1)
    candidates = load_all_candidates(filter=False, city='Oakland', limit = -1)
    #candidates = load_all_candidates(filter=False, city='all', limit = -1)
    #candidates = load_all_candidates(filter=True, city='all', missing=['coffee'], limit = -1)
    #candidates = load_all_candidates()
    #score_records(candidates,['coffee','restaurant','convenience_store', 'bart'])
    #score_records(candidates,['bart'])
    score_records(candidates,['crime'])
    #score_records(candidates,['coffee'])
    #score_records(candidates,'all')