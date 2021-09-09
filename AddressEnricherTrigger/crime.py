import os
import requests
import json
from datetime import datetime, timedelta
from locations import geocode
from util import map_range, quarter_mile, half_mile, mile
from geopy import distance

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
oak_crime = [] #stores oakland crimes if they are preloaded


now = datetime.now()
start_time = datetime.now()
crime_eval_period = 90
one_week_seconds = timedelta(weeks=1).total_seconds()
crime_eval_period_seconds = timedelta(days=crime_eval_period).total_seconds()
crime_eval_distance = half_mile

def get_crime(address, geo, api='mixed'):
    #print('get_crime ({},{})'.format(address, geo))
    start_time = datetime.now()

    if api == 'mixed':
        if 'Oakland' in address:
            if len(oak_crime) == 0:
                #return get_crime_soda(geo, 'oakland')
                return None
            else:
                return filter_and_score_crime(geo, oak_crime, 'oakland')
        elif 'Berkeley' in address:
            return get_crime_soda(geo, 'berkeley')
        else:
            return get_crime_alameda(geo)
    elif api == 'crimeometer':
        return get_crime_crimeometer(geo)
    else:
        return None

def get_crime_soda(geo, location):

    if location == 'oakland':
        location_column = 'location_1'
        crime_type_column = 'crimetype'
        datetime_column = 'datetime'
        additional = 'and policebeat in {}'.format(tuple(oakland_police_beats))
        #location_search = 'within_circle({},{},{},{}) and '.format(location_column, geo[0],geo[1],round(half_mile))
        location_search = ''
    else:
        location_column = 'block_location'
        crime_type_column = 'offense'
        datetime_column = 'eventdt'
        location_search = 'within_circle({},{},{},{}) and '.format(location_column, geo[0],geo[1],round(half_mile))
        additional = ''

    days_ago = (now - timedelta(days=crime_eval_period)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    params = {  '$where':'{}{} > "{}" and {} in {} {}'
                    .format(location_search, datetime_column, days_ago, crime_type_column,
                            tuple(violent_crimes + non_violent_crimes), additional),
                '$limit':'5000'
    }
    #print('Crime parameters {}'.format(params))
    req = requests.get(url=crime_url[location], params=params, headers=crime_header[location])
    req.raise_for_status()
    crimes = json.loads(req.text)
    #print('Crimes {}'.format(json.dumps(crimes, indent=2)))
    return filter_and_score_crime(geo,crimes,location)

def get_crime_alameda(geo):
    return None

def get_crime_crimeometer(geo):

    cur_page = 1
    days_ago = (now - timedelta(days=crime_eval_period)).strftime("%Y-%m-%d %H:%M:%S")
    params = {
        'lat': geo[0],
        'lon': geo[1],
        'distance': '{}m'.format(round(half_mile)),
        'datetime_ini': days_ago,
        'datetime_end' : (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        'page' : cur_page
    }
    req = requests.get(url=crime_url['crimeometer'], params=params, headers=crime_header['crimeometer'])
    req.raise_for_status()
    req = json.loads(req.text)
    #print(req)
    crimes = req['incidents']

    while cur_page < req['total_pages']:
        cur_page += 1
        params['page'] = cur_page
        req = requests.get(url=crime_url['crimeometer'], params=params, headers=crime_header['crimeometer'])
        req.raise_for_status()
        req = json.loads(req.text)
        crimes.extend(req['incidents'])

    return filter_and_score_crime(geo,crimes,'crimeometer')


def filter_and_score_crime(start,crimes,location):
    nv, nv_raw = 0, 0
    v, v_raw = 0, 0
    geo_sub_column = True
    latitude_column = 'latitude'
    longitude_column = 'longitude'
    city_state_in_address = False

    if location == 'oakland':
        location_column = 'location_1'
        crime_type_column = 'crimetype'
        datetime_column = 'datetime'
        address_column = 'address'
    elif location == 'berkeley':
        location_column = 'block_location'
        crime_type_column = 'offense'
        datetime_column = 'eventdt'
        address_column = 'blkaddr'
    elif location == 'crimeometer':
        location_column = 'incident_latitude'
        crime_type_column = 'incident_offense_code'
        datetime_column = 'incident_date'
        address_column = 'incident_address'
        geo_sub_column = False
        latitude_column = 'incident_latitude'
        longitude_column = 'incident_longitude'
        city_state_in_address = True

    for crime in crimes:
        if geo_sub_column and location_column in crime:
            crime_geo = (crime[location_column][latitude_column],crime[location_column][longitude_column])
        elif not geo_sub_column and latitude_column in crime and longitude_column in crime:
            crime_geo = (crime[latitude_column],crime[longitude_column])
        else:
            crime_geo = None

        if address_column not in crime:
            print('crime missing address field - {}'.format(crime))
            crime_address = None
        else:
            if city_state_in_address:
                crime_address = crime[address_column]
            else:
                crime_address = '{}, {}, {}'.format(crime[address_column],crime['city'],crime['state'])

        try:
            crime_time = datetime.strptime(crime[datetime_column], '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                crime_time = datetime.strptime(crime[datetime_column], '%Y-%m-%dT%H:%M:%S.%fZ')
            except:
                print('could not convert crime time - {}'.format(crime[datetime_column]))
                break
        except:
            print('could not convert crime time - {}'.format(crime[datetime_column]))
            break

        time = (now - crime_time).total_seconds()
        distance = get_crime_distance(start, crime_geo, crime_address)

        if time <= crime_eval_period_seconds and distance <= crime_eval_distance:
            if crime[crime_type_column] in non_violent_crimes:
                nv_raw += 1
                nv += grade_crime_time(time)
                nv += grade_crime_distance(distance)
            elif crime[crime_type_column] in violent_crimes:
                v_raw += 1
                v += grade_crime_time(time)
                v += grade_crime_distance(distance)
            elif crime[crime_type_column] == 'N/A':
                print('found a crimeometer crime with code N/A - {}'.format(json.dumps(crimes, indent=2)))
    print('Pulled and scored crime for {} in {}'.format(start, datetime.now() - start_time))
    print('Raw crime counts - violent {}, non-violent {}'.format(v_raw, nv_raw))
    return {'violent':v, 'non-violent':nv}

def grade_crime_time(sec):
    if sec < one_week_seconds:
        sec = one_week_seconds

    return map_range(sec, one_week_seconds, crime_eval_period_seconds, 0, 0.5)


def get_crime_distance(coord_1, coord_2, address):
    if coord_2:
        d = distance.distance(coord_1, coord_2).meters
    else:
        geo = geocode(address=address, api="mapquest")['geo']
        d = distance.distance(coord_1, geo).meters
    return d

def grade_crime_distance(d):

    if d < (quarter_mile / 2):
        d = (quarter_mile / 2)

    return 0.5 - map_range(d, (quarter_mile / 2), crime_eval_distance, 0, 0.5)

def load_oakland_crime(api='mapquest', config=None):
    """
        This method is called when the python program is executed as a script
        Loads all oakland crimes and geocodes them
    """
    from boto3.dynamodb.conditions import Key
    import boto3

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Crimes')

    scan_kwargs = {'FilterExpression': Key('city').eq('Oakland'),
                    'ProjectionExpression': 'details'}
    done = False
    start_key = None

    start = datetime.now()
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        if not response.get('Count',0) == 0:
            oak_crime.extend([item['details'] for item in response['Items'] if 'details' in item])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return