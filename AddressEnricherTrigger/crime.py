import os
import requests
import json
from datetime import datetime, timedelta
from locations import geocode
from util import get_distance, map_range

soda_app_token = os.environ['soda_app_token']
soda_api_secret = os.environ['soda_api_secret']
soda_secret_token = os.environ['soda_secret_token']
soda_url = os.environ['soda_url']
soda_header = {'Accept':'application/json','X-App-Token':soda_app_token}
non_violent_crimes = os.environ['non_violent_crimes'].split(";")
violent_crimes = os.environ['violent_crimes'].split(";")
police_beats = os.environ['oakland_police_beats'].split(",")


now = datetime.now()

def get_crime(geo):
    days_ago = (now - timedelta(days=45)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    location='within_circle(location,{},{},{})'.format(geo[0],geo[1],810)
    params = {  '$where':'datetime > "{}" and crimetype in{} and policebeat in {}'
                    .format(days_ago,
                            tuple(violent_crimes + non_violent_crimes),
                            tuple(police_beats)),
                '$limit':'5000'
    }
    #print('Crime parameters {}'.format(params))
    req = requests.get(url=soda_url, params=params, headers=soda_header)
    crimes = json.loads(req.text)
    #print('Crimes {}'.format(json.dumps(crimes, indent=2)))
    return filter_and_score_crime(geo,crimes)

def filter_and_score_crime(start,crimes):
    nv = 0
    v = 0
    for crime in crimes:
        if 'location' in crime:
            crime_geo = (crime['location_1']['latitude'],crime['location_1']['longitude'])
            crime_address = '{}, {}, {}'.format(crime['address'],crime['city'],crime['state'])
        else:
            crime_geo = None
            crime_address = '{}, {}, {}'.format(crime['address'],crime['city'],crime['state'])

        if crime['crimetype'] in non_violent_crimes:
            crime_time = datetime.strptime(crime['datetime'], '%Y-%m-%dT%H:%M:%S.%f')
            nv += grade_crime_time(crime_time)
            #nv += grade_crime_distance(start, crime_geo, crime_address)
        elif crime['crimetype'] in violent_crimes:
            v += grade_crime_time(crime_time)
            #v += grade_crime_distance(start, crime_geo, crime_address)
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
        geo = geocode(address=coord_2, api="mapquest")['geo']
        distance = get_distance(coord_1, geo)

    if distance < 0.25: # roughly 5 city blocks
        distance = 0.25

    return 0.5 - map_range(distance,0.5,1,0,0.5)