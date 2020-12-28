import os
import requests
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

if __name__ == '__main__':
    import yaml
    with open('template.yaml', 'r') as stream:
        var = yaml.load(stream, Loader=yaml.FullLoader)
        for k,v in var['Resources']['AddressEnricherTrigger']['Properties']['Environment']['Variables'].items():
            os.environ[k] = v

from locations import geocode
from util import map_range, quarter_mile, half_mile, mile
from geopy import distance
from decimal import Decimal


crime_app_token = json.loads(os.environ['crime_app_token'])
crime_api_secret = json.loads(os.environ['crime_api_secret'])
crime_secret_token = json.loads(os.environ['crime_secret_token'])
crime_url = json.loads(os.environ['crime_url'])
crime_header = {
    'oakland' : {'Accept':'application/json','X-App-Token':crime_app_token['oakland']},
    'berkeley' : {'Accept':'application/json','X-App-Token':crime_app_token['berkeley']},
    'crimeometer' : {'Accept':'application/json','x-api-key':crime_api_secret['crimeometer']},
}
non_violent_crimes = os.environ['non_violent_crimes'].split(";")
violent_crimes = os.environ['violent_crimes'].split(";")
oakland_police_beats = os.environ['oakland_police_beats'].split(",")

crime_eval_period = 90


def get_latest_saved_crime():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Crimes')

    results = table.query(
            KeyConditionExpression=Key('city').eq('Oakland'),
            ProjectionExpression='#s1',
            ExpressionAttributeNames = {'#s1':'datetime'},
            Limit=1,
            ScanIndexForward=False
        )

    if results['Count'] == 0:
        print('No previously saved crimes - will start from {} days ago'.format(crime_eval_period))
        return None
    else:
        print ('Most recent saved crime - {}'.format(datetime.fromtimestamp(int(results['Items'][0]['datetime']))))
        return int(results['Items'][0]['datetime'])

def load_oakland_crime(start):
    """
        This method is called when the python program is executed as a script
        Loads all oakland crimes and geocodes them
    """
    if not start:
        start = (datetime.now() - timedelta(days=crime_eval_period)).timestamp()

    params = {
        '$where':'datetime > "{}" and datetime <= "{}" and crimetype in {}'.format(datetime.fromtimestamp(start).strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                                                                datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                                                                tuple(violent_crimes + non_violent_crimes)),
        '$limit':50000
    }
    print('Load Oakland Crime - parameters {}'.format(params))
    req = requests.get(url=crime_url['oakland'], params=params, headers=crime_header['oakland'])
    crimes = json.loads(req.text)
    print('Found {} Oakland crimes to geocode'.format(len(crimes)))

    return crimes

def remove_existing_crimes(crimes):
    from boto3.dynamodb.conditions import Key

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Crimes')

    scan_kwargs = {'FilterExpression': Key('city').eq('Oakland'),
                    'ProjectionExpression': 'casenumber'}
    done = False
    start_key = None
    existing_crimes = []

    start = datetime.now()
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        if not response.get('Count',0) == 0:
            existing_crimes += [item['casenumber'] for item in response['Items'] if 'casenumber' in item]
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    new_crimes = []
    for crime in crimes:
        if crime['casenumber'] not in existing_crimes:
            new_crimes.append(crime)

    print('Of {} crimes, {} are new (processed in {})'.format(len(crimes), len(new_crimes), datetime.now() - start))
    return new_crimes

def geocode_crimes(crimes, api='mapquest', config=None):
    if api == 'mapquest':
        report = 100
    else:
        report = 10

    geocoded_crimes = []
    address_batch = []
    function_start = datetime.now()
    start = datetime.now()
    for idx, crime in enumerate(crimes):
        if crime['crimetype'] in non_violent_crimes or crime['crimetype'] in violent_crimes:
            if api == 'mapquest':
                address_batch.append('{}, {}, {}'.format(crime['address'], crime['city'], crime['state']))
                if len(address_batch) == 10:
                    geos = geocode(address=address_batch, api=api, batch=True)
                    for geo in geos:
                        crime = next(item for item in crimes
                                    if '{}, {}, {}'.format(item["address"], item["city"], item["state"]) == geo['formated_address'])
                        crime['location_1'] = {'latitude':geo['geo'][0],'longitude':geo['geo'][1]}
                        geocoded_crimes.append(crime)
                    address_batch = []
            else:
                address = '{}, {}, {}'.format(crime['address'], crime['city'], crime['state'])
                geo = geocode(address=address, api=api, batch=False, config=config)
                crime['location_1'] = {'latitude':geo['geo'][0],'longitude':geo['geo'][1]}
                geocoded_crimes.append(crime)
        if (idx+1) % report == 0:
            print('Filtered {} and Geocoded {} total crimes in {}'.format(idx+1, len(geocoded_crimes), datetime.now() - start))
            start = datetime.now()

    if len(address_batch) > 0: #left over batched addresses to geocode
        geos = geocode(address=address_batch, api=api, batch=True)
        for geo in geos:
            crime = next(item for item in crimes
                        if '{}, {}, {}'.format(item["address"], item["city"], item["state"]) == geo['formated_address'])
            crime['location_1'] = {'latitude':geo['geo'][0],'longitude':geo['geo'][1]}
            geocoded_crimes.append(crime)

    print('Geocoded {} crimes in {} total'.format(len(geocoded_crimes), datetime.now() - function_start))
    return geocoded_crimes

def save_crimes(crimes):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Crimes')

    start = datetime.now()
    for crime in crimes:
        item = {
            'city' : crime['city'],
            'datetime' : int(datetime.strptime(crime['datetime'],"%Y-%m-%dT%H:%M:%S.%f").timestamp()),
            'casenumber' : crime['casenumber'],
            'details' : json.loads(json.dumps(crime), parse_float=Decimal)
        }
        table.put_item(Item=item)

    print('Saved {} crimes in {}'.format(len(crimes), datetime.now() - start))
    return

if __name__ == '__main__':


    crimes = load_oakland_crime(get_latest_saved_crime())
    if crimes:
        new_crimes = remove_existing_crimes(crimes)
        if new_crimes:
            new_crimes = geocode_crimes(new_crimes)
            save_crimes(new_crimes)