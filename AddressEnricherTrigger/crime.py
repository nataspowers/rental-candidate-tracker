import os
import requests
import json
from datetime import datetime, timedelta
from locations import geocode
from util import get_distance, map_range

crime_app_token = json.loads(os.environ['crime_app_token'])
crime_api_secret = json.loads(os.environ['crime_api_secret'])
crime_secret_token = json.loads(os.environ['crime_secret_token'])
crime_url = json.loads(os.environ['crime_url'])
crime_header = {
    'oakland' : {'Accept':'application/json','X-App-Token':crime_app_token['oakland']},
    'berkeley' : {'Accept':'application/json','X-App-Token':crime_app_token['berkeley']}
}
non_violent_crimes = os.environ['non_violent_crimes'].split(";")
violent_crimes = os.environ['violent_crimes'].split(";")
oakland_police_beats = os.environ['oakland_police_beats'].split(",")
oak_crime = [] #stores oakland crimes if they are preloaded


now = datetime.now()

def get_crime(address, geo):
    #print('get_crime ({},{})'.format(address, geo))
    if 'Oakland' in address:
        if len(oak_crime) == 0:
            return get_crime_soda(geo, 'oakland')
        else:
            return filter_and_score_crime(geo, oak_crime, 'oakland')
    elif 'Berkeley' in address:
        return get_crime_soda(geo, 'berkeley')
    else:
        return get_crime_alameda(geo)

def get_crime_soda(geo, location):

    if location == 'oakland':
        location_column = 'location_1'
        crime_type_column = 'crimetype'
        datetime_column = 'datetime'
        additional = 'and policebeat in {}'.format(tuple(oakland_police_beats))
        #location_search = 'within_circle({},{},{},{}) and '.format(location_column, geo[0],geo[1],810)
        location_search = ''
    else:
        location_column = 'block_location'
        crime_type_column = 'offense'
        datetime_column = 'eventdt'
        location_search = 'within_circle({},{},{},{}) and '.format(location_column, geo[0],geo[1],810)
        additional = ''

    days_ago = (now - timedelta(days=45)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    params = {  '$where':'{}{} > "{}" and {} in {} {}'
                    .format(location_search, datetime_column, days_ago, crime_type_column,
                            tuple(violent_crimes + non_violent_crimes), additional),
                '$limit':'5000'
    }
    #print('Crime parameters {}'.format(params))
    req = requests.get(url=crime_url[location], params=params, headers=crime_header[location])
    crimes = json.loads(req.text)
    #print('Crimes {}'.format(json.dumps(crimes, indent=2)))
    return filter_and_score_crime(geo,crimes,location)

def get_crime_alameda(geo):
    return None

def filter_and_score_crime(start,crimes,location):
    nv = 0
    v = 0
    if location == 'oakland':
        location_column = 'location_1'
        crime_type_column = 'crimetype'
        datetime_column = 'datetime'
        address_column = 'address'
    else:
        location_column = 'block_location'
        crime_type_column = 'offense'
        datetime_column = 'eventdt'
        address_column = 'blkaddr'

    for crime in crimes:
        if location_column in crime:
            crime_geo = (crime[location_column]['latitude'],crime[location_column]['longitude'])
            if not crime.get(address_column,False):
                print('crime missing address field - {}'.format(crime))
                crime_address = None
            else:
                crime_address = '{}, {}, {}'.format(crime[address_column],crime['city'],crime['state'])
        else:
            crime_geo = None
            crime_address = '{}, {}, {}'.format(crime[address_column],crime['city'],crime['state'])


        crime_time = datetime.strptime(crime[datetime_column], '%Y-%m-%dT%H:%M:%S.%f')
        if crime[crime_type_column] in non_violent_crimes:
            nv += grade_crime_time(crime_time)
            nv += grade_crime_distance(start, crime_geo, crime_address)
        elif crime[crime_type_column] in violent_crimes:
            v += grade_crime_time(crime_time)
            v += grade_crime_distance(start, crime_geo, crime_address)
    return {'violent':v, 'non-violent':nv}

def grade_crime_time(ts):
    time = now - ts
    if time < timedelta(weeks=1):
        time = timedelta(weeks=1)

    one_week = timedelta(weeks=1).total_seconds()
    ninety_days = timedelta(days=90).total_seconds()

    return map_range(time.total_seconds(),one_week,ninety_days,0,0.5)

def grade_crime_distance(coord_1,coord_2,address):
    if coord_2:
        distance = get_distance(coord_1,coord_2)
    else:
        geo = geocode(address=address, api="mapquest")['geo']
        distance = get_distance(coord_1, geo)

    if distance < 0.25: # roughly 5 city blocks - everything that close is the same risk
        distance = 0.25

    if distance <= 1: #ignore crime more than 1 mile away
        return 0.5 - map_range(distance,0.25,1,0,0.5)
    else:
        return 0

def load_oakland_crime():
    """
        This method is called when the python program is executed as a script
        Loads all oakland crimes and geocodes them
    """
    days_ago = (now - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    params = {
        '$where':'{} > "{}" and {} in {}'.format('datetime', days_ago, 'crimetype',
                            tuple(violent_crimes + non_violent_crimes)),
        '$limit':50000
    }
    print('Load Oakland Crime - parameters {}'.format(params))
    req = requests.get(url=crime_url['oakland'], params=params, headers=crime_header['oakland'])
    crimes = json.loads(req.text)
    print('Found {} Oakland crimes to filter and geocode'.format(len(crimes)))

    address_batch = []
    start = datetime.now()
    for idx, crime in enumerate(crimes):
        if crime['crimetype'] in non_violent_crimes or crime['crimetype'] in violent_crimes:
            address_batch.append('{}, {}, {}'.format(crime['address'], crime['city'], crime['state']))
        if len(address_batch) == 10:
            geos = geocode(address=address_batch, api="mapquest", batch=True)
            for geo in geos:
                crime = next(item for item in crimes
                                if '{}, {}, {}'.format(item["address"], item["city"], item["state"]) == geo['formated_address'])
                crime['location_1'] = {'latitude':geo['geo'][0],'longitude':geo['geo'][1]}
                oak_crime.append(crime)
            address_batch = []
        if (idx+1) % 100 == 0:
            print('Filtered {} and Geocoded {} total crimes in {}'.format(idx+1, len(oak_crime), datetime.now() - start))
            start = datetime.now()

    return