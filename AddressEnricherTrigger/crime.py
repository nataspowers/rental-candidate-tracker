import os
import requests
import json
from datetime import datetime, timedelta
from util import get_distance

soda_app_token = os.environ['soda_app_token']
soda_api_secret = os.environ['soda_api_secret']
soda_secret_token = os.environ['soda_secret_token']
soda_url = os.environ['soda_url']
soda_header = {'Accept':'application/json','X-App-Token':soda_app_token}
non_violent_crimes = os.environ['non_violent_crimes'].split(";")
violent_crimes = os.environ['violent_crimes'].split(";")

now = datetime.now()

def get_crime(geo):
    location='within_circle(location_1,{},{},{})'.format(geo[0],geo[1],810)
    params = {'$where':'{} and crimetype in{}'.format(location, violent_crimes + non_violent_crimes)}
    print('Crime parameters {}'.format(params))
    req = requests.get(url=soda_url, params=params, headers=soda_header)
    crimes = json.loads(req.text)
    #print('Crimes {}'.format(json.dumps(crimes, indent=2)))
    return filter_and_score_crime(geo,crimes)

def filter_and_score_crime(start,crimes):
    nv = 0
    v = 0
    for crime in crimes:
        crime_geo = (crime['location_1']['latitude'],crime['location_1']['longitude'])
        if crime['crimetype'] in non_violent_crimes:
            crime_time = datetime.strptime(crime['datetime'], '%Y-%m-%dT%H:%M:%S.%f')
            nv += grade_crime(start,crime_geo,crime_time)
        if crime['crimetype'] in violent_crimes:
            v += grade_crime(start,crime_geo,crime_time)
    return {'violent':v, 'non-violent':nv}

def grade_crime(coord_1,coord_2,ts):
    distance = get_distance(coord_1,coord_2)
    if distance < 0.25: # roughly 5 city blocks
        distance = 0.25
    # old range was 0.25 to 1, new range is 0 to 0.5
    dist_score = 0.5 - (((distance - 0.25) * (0.5 - 0)) / (1 - 0.25)) + 0
    #print('distance {} = distance score {}'.format(distance,dist_score))

    time = now - ts
    if time < timedelta(weeks=1):
        time = timedelta(weeks=1)
    #old range was 1 week to 90 day, new range is 0 to 0.5
    time_score = 0.5 - (((time - timedelta(weeks=1)).total_seconds() * (0.5-0)) / (timedelta(days=165)-timedelta(weeks=1)).total_seconds()) + 0
    #print('time = {}, time score {}'.format(time, time_score))
    return dist_score + time_score