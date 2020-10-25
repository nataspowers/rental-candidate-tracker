import json
import googlemaps
from datetime import datetime, timedelta
from geopy import distance
import requests
import boto3
import os

print('Loading function')
gmaps = googlemaps.Client(key=os.environ['gmap_key'])
yelp_url = os.environ['yelp_url']
yelp_header = {'Authorization': 'Bearer %s' % os.environ['yelp_api_key']}

ws_api_key = os.environ['ws_api_key']
ws_url = os.environ['ws_url']
ws_header = os.environ['ws_header']

soda_app_token = os.environ['soda_app_token']
soda_api_secret = os.environ['soda_api_secret']
soda_secret_token = os.environ['soda_secret_token']
soda_url = os.environ['soda_url']
soda_header = {'Accept':'application/json','X-App-Token':soda_app_token}
non_violent_crimes = os.environ['non_violent_crimes'].split(";")
violent_crimes = os.environ['violent_crimes'].split(";")

friend_geo = os.environ['friend'].split(",")
work_geo = os.environ['work'].split(",")

now = datetime.now()


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('candidates')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    sources = []
    for record in event['Records']:
        address = None
        #print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
        if record['eventName'] == 'INSERT':
            address = record['dynamodb']['Keys']['Id']['N']

        if record['eventName'] == 'MODIFY':
            if not 'dist_m_r' in record['dynamodb']['NewImage']:
                address = record['dynamodb']['NewImage']['Id']['N']

        if address != None:
            sources.append(address)

    #drive_times = get_drive_time_friend(sources)
    #print('Drive to Friend {}'.format(json.dumps(drive_times, indent=2)))
    #transit_commute = get_commute_transit(sources)
    #print('Commute Transit {}'.format(json.dumps(transit_commute, indent=2)))
    #drive_commute = get_commute_drive(sources)
    #print('Commute Drive {}'.format(json.dumps(drive_commute, indent=2)))

    for address in sources:
        address_detail = gmaps.geocode(address=address)
        #print(json.dumps(address_detail, indent=2))
        formatted_address = address_detail[0]['formatted_address']
        geo = address_detail[0]['geometry']['location']
        geo = (geo['lat'],geo['lng'])

        #scores = get_walk_score(geo, formatted_address)
        #print('Scores {}'.format(json.dumps(scores, indent=2)))

        #crime = get_crime(geo)
        #print('Crime {}'.format(json.dumps(crime, indent=2)))

        #coffee = get_coffee_shops(geo)
        #print('Coffee {}'.format(json.dumps(coffee, indent=2)))

        #restaurant = get_restaurants(geo)
        #print('Restaurants {}'.format(json.dumps(restaurant, indent=2)))

        #stores = get_convenience_store(geo)
        #print('Convenience Stores {}'.format(json.dumps(stores, indent=2)))

        #friend_commute = fetch_drive_time(formatted_address, drive_times)
        #commute = {'transit' : fetch_drive_time(formatted_address, transit_commute),
        #            'drive' : fetch_drive_time(formatted_address, drive_commute)}
        #places = {'coffee' : coffee, 'restaurant' : restaurant, 'convenience_store':stores}

        #res = update_table(address, friend_commute, commute, places, scores, crime)

    return 'Successfully processed {} records.'.format(len(event['Records']))

def get_walk_score(geo, address):
    params = {'lat': geo[0],
              'lon': geo[1],
              'address': address,
              'wsapikey': ws_api_key,
              'transit': 1,
              'bike': 1,
              'format': 'json'
    }
    req = requests.get(url=ws_url, params=params, headers=ws_header)
    scores = json.loads(req.text)
    if scores['status'] == 1:
        return {'walk':scores['walkscore'],
                'transit':scores['transit'].get('score',None),
                'bike':scores['bike'].get('score',None)}
    else:
        return None

def get_crime(geo):
    location='within_circle(location_1,{},{},{})'.format(geo[0],geo[1],810)
    params = {'$where':'{} and crimetype in{}'.format(location, violent_crimes + non_violent_crimes)}
    print('Crime parameters {}'.format(params))
    req = requests.get(url=soda_url, params=params, headers=soda_header)
    crimes = json.loads(req.text)
    #print('Crimes {}'.format(json.dumps(crimes, indent=2)))
    return filter_and_score_crime(geo,crimes)

def get_drive_time_m_r(start):
    """
    @start: location to calculate driving distance from
    """

    now = datetime.now()
    weekend = get_next_weekday(now.strftime("%Y-%m-%d"), 5) + " 14:00:00"
    weekend = datetime.strptime(weekend, '%Y-%m-%d %H:%M:%S')
    distance = gmaps.distance_matrix(origins=start,
                        destinations=friend_geo,
                        mode="driving",
                        units="imperial",
                        departure_time=weekend)
    return distance

def get_commute_transit(start):
    """
    @start: location to calculate driving distance from
    """
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 08:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = gmaps.distance_matrix(origins=start,
                        destinations=work_geo,
                        mode="transit",
                        units="imperial",
                        transit_routing_preference="fewer_transfers",
                        arrival_time=monday_morning)
    return distance

def get_commute_drive(start):
    """
    @start: location to calculate driving distance from
    """
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 07:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = gmaps.distance_matrix(origins=start,
                                    destinations=work_geo,
                                    mode="driving",
                                    units="imperial",
                                    departure_time=monday_morning)
    return distance


def get_coffee_shops(start):
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius':805,
              'categories':'coffee',
              'sort_by':'distance'
    }
    req = requests.get(url=yelp_url, params=params, headers=yelp_header)
    coffee_shops = json.loads(req.text)
    #print(json.dumps(coffee_shops, indent=2))
    results = filter_places('yelp',coffee_shops, start, 3.5)
    return results

def get_restaurants(start):
    restauraunts = gmaps.places(query="dinner",
                                location=start,
                                radius="1610",
                                type="restaurant")
    #print(json.dumps(restauraunts, indent=2))
    results = filter_places('google',restauraunts, start, 3.5)
    return results

def get_convenience_store(start):
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius':1610,
              'categories':'convenience',
              'sort_by':'distance'
    }
    req = requests.get(url=yelp_url, params=params, headers=yelp_header)
    stores = json.loads(req.text)
    #print(json.dumps(stores, indent=2))
    results = filter_places('yelp', stores, start, 3.0)
    return results

def filter_places(api,places,start,min_rating):
    highest_rated = {}
    closest = {}
    closest_dist = 5000
    result_term = 'results' if api == 'google' else 'businesses'
    review_count_term = 'user_ratings_total' if api == 'google' else 'review_count'
    for idx, place in enumerate(places[result_term]):
        #print ('place {}'.format(idx))
        if api == 'google':
            if place['business_status'] == 'CLOSED_PERMANENTLY':
                #print('It"s closed bitches - {}'.format(place['business_status']))
                continue
        else:
            if place['is_closed'] == 'true':
                #print('It"s closed bitches - {}'.format(place['is_closed']))
                continue
        if place['rating'] < min_rating and place[review_count_term] >= 10:
            #print('It"s shit - Rating {} from {} reviews'.format(place['rating'],place[review_count_term]))
            continue

        if place['rating'] > highest_rated.get('rating',0):
            highest_rated = place

        if api == 'google':
            place_geo = (place['geometry']['location']['lat'],place['geometry']['location']['lng'])
        else:
            place_geo = (place['coordinates']['latitude'],place['coordinates']['longitude'])
        place_dist = get_distance(place_geo,start)
        if place_dist < closest_dist:
            closest = place
            closest_dist = place_dist
    #print('Highest Rated ({}):'.format(highest_rated.get('rating',0)))
    #print(json.dumps(highest_rated, indent=2))
    #print('Closest ({}mi):'.format(closest_dist))
    #print(json.dumps(closest, indent=2))
    return {'highest_rated':{'place':highest_rated,'value':highest_rated['rating']},
            'closest':{'place':closest,'value':closest_dist}}

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

def get_distance(coords_1, coords_2):
    #print('start {} - end {}'.format(coords_1,coords_2))
    return distance.distance(coords_1, coords_2).miles

def get_next_weekday(startdate, weekday):
    """
    @startdate: given date, in format '2013-05-25'
    @weekday: week day as a integer, between 0 (Monday) to 6 (Sunday)
    """
    d = datetime.strptime(startdate, '%Y-%m-%d')
    t = timedelta((7 + weekday - d.weekday()) % 7)
    return (d + t).strftime('%Y-%m-%d')

def fetch_drive_time(item_to_find, matrix):
    #print('looking for: {}'.format(item_to_find))
    #print('In Matrix {}'.format(json.dumps(matrix, indent=2)))
    index = matrix['origin_addresses'].index(item_to_find)
    #print('index found = {}'.format(index))
    #print('Matrix {}'.format(json.dumps(matrix['rows'], indent=2)))
    return matrix['rows'][index]['elements'][0]

def update_table (key, friend_drive, commute, places, score, crime):
    update_expr = 'set friend_drive = :val1, set commute = :val2, places = :val3, walk_score = :val4, crime = :val5'
    response = table.update_item(
                Key={'address': key},
                UpdateExpression=update_expr,
                ExpressionAttributeValues={
                   ':val1': friend_drive,
                   ':val2': commute,
                   ':val3': places,
                   ':val4': score,
                   ':val5': crime
                }
            )
    return response