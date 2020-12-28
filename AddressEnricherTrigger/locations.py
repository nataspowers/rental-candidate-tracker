import googlemaps
import requests
import json
import os
from datetime import datetime
from geopy.geocoders import get_geocoder_for_service

from util import get_distance, mile, half_mile

gmaps = googlemaps.Client(key=os.environ['gmap_key'])

yelp_url = os.environ['yelp_url']
yelp_header = {'Authorization': 'Bearer %s' % os.environ['yelp_api_key']}
dinner_categories = os.environ['dinner_categories'].split(",")
lunch_categories = os.environ['lunch_categories'].split(",")
brunch_categories = os.environ['brunch_categories'].split(",")

mapquest_key = os.environ['mapquest_key']
mapquest_url = os.environ['mapquest_url']

quarter_mile = 402.336
half_mile = quarter_mile * 2
mile = half_mile * 2

def get_coffee_shops(start):
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius': round(half_mile),
              'categories':'coffee',
              'sort_by':'distance'
    }

    coffee_shops = []
    received, total, retry = -1, 0, False
    s = datetime.now()
    while received < total and received < 1000:
        try:
            req = requests.get(url=yelp_url, params=params, headers=yelp_header)
            req.raise_for_status()
        except requests.HTTPError:
            if not retry:
                retry = True
        response = json.loads(req.text)
        businesses = response.get('businesses',[])
        if businesses:
            coffee_shops += response.get('businesses',[])
            total = response['total']
            received += len(businesses)
            params['offset'] = received
        elif response.get('error',{}).get('code','Other Error') == 'INTERNAL_ERROR' and not retry:
            retry = True
        else: # a different error, no businesses, or we've retried once already
            print('Terminating try to get coffee for {} - last response {}'.format(start, response))
            break


    print('Pulled {} coffee shops for {} in {}'.format(len(coffee_shops), start, datetime.now() - s))
    if coffee_shops:
        return analyze_places(coffee_shops, cat_stats=False)
    else:
        print('No coffee shops found...')
        return None


def get_restaurants(start):
    catagories = ','.join(str(s) for s in (dinner_categories + lunch_categories + brunch_categories))
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius': round(mile),
              'categories':catagories,
              'sort_by':'distance'
    }

    restauraunts = []
    received, total, retry = -1, 0, False
    s = datetime.now()
    while received < total and received < 1000:
        try:
            req = requests.get(url=yelp_url, params=params, headers=yelp_header)
            req.raise_for_status()
        except requests.HTTPError:
            if not retry:
                retry = True
        response = json.loads(req.text)
        businesses = response.get('businesses',[])
        if businesses:
            restauraunts += response.get('businesses',[])
            total = response['total']
            received += len(businesses)
            params['offset'] = received
        elif response.get('error',{}).get('code','Other Error') == 'INTERNAL_ERROR' and not retry:
            retry = True
        else: # a different error, no businesses, or we've retried
            print('Terminating try to get restauraunts for {} - last response {}'.format(start, response))
            break

        #print(json.dumps(response, indent=2))

    print('Pulled {} restauraunts for {} in {}'.format(len(restauraunts), start, datetime.now() - s))

    if restauraunts:
        return analyze_places(restauraunts)
    else:
        print('No restauraunts found...')
        return None

def get_convenience_store(start):
    retry = False
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius': round(mile),
              'categories':'convenience',
              'sort_by':'distance'
    }
    while 1:
        try:
            req = requests.get(url=yelp_url, params=params, headers=yelp_header)
            req.raise_for_status()
        except:
            if not retry:
                retry = True
                continue
        stores = json.loads(req.text)
        if stores.get('error',{}).get('code','Other Error') == 'INTERNAL_ERROR' and not retry:
            retry = True
            continue
        break
    #print(json.dumps(stores, indent=2))
    results = filter_places('yelp', stores, start, 3.0)
    return results

def get_bart(start):

    bart = gmaps.places(query='bart',
                    location=start,
                    radius=5 * round(mile),
                    type='transit_station')

    results = filter_places('google', bart, start, 0.0)
    #print(json.dumps(results, indent=2))
    return results['closest']

def filter_places(api, places, start, min_rating):
    highest_rated = {}
    closest = {}
    closest_dist = 5000
    result_term = 'results' if api == 'google' else 'businesses'
    review_count_term = 'user_ratings_total' if api == 'google' else 'review_count'

    if not places.get(result_term,False):
        print ('No places of place type for {} - places = {}'.format(start, places))
        return None

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
    return {'highest_rated':{'place':highest_rated,'value':highest_rated.get('rating',0)},
            'closest':{'place':closest,'value':closest_dist}}

def analyze_places(places, cat_stats=True, dist_stats=True):
    results = {}
    results['total'] = len([p for p in places if not_closed(p) ])
    #print('total places {}'.format(results['total']))

    for t in ['rating','distance']:
        results[t] = {'sum' : sum([ p.get(t,0) for p in places if not_closed(p) ])}
        results[t]['average'] = results[t]['sum'] / results['total']
        results[t]['min'] = min([ p.get(t,0) for p in places if not_closed(p) ])
        results[t]['max'] = max([ p.get(t,0) for p in places if not_closed(p) ])

    #print('results after min/max/avg{}'.format(json.dumps(results, indent=2)))

    highest_rating = results['rating']['max']
    #print('histest rating {}'.format(highest_rating))
    highest_rated = [p for p in places if matches('rating-no-round',highest_rating,p)]
    if highest_rated:
        highest_rated = highest_rated[0]
    else:
        print('no matches - {}'.format(json.dumps(places, indent=2)))
    closest_dist = results['distance']['min']
    closest = [p for p in places if matches('distance',closest_dist,p)][0]

    results['highest_rated'] = {'place':highest_rated,'value':highest_rating}
    results['closest'] = {'place':closest,'value':closest_dist}

    keys = ['rating', 'price']
    if cat_stats:
        keys.append('categories')
    if dist_stats:
        keys.append('distance-group')

    for key_type in keys:
        res = {}
        keys = unique_flatten([get_list(key_type, p) for p in places])
        for k in keys:
            if k not in res.keys():
                res[k] = {'total' : len([ p for p in places if matches(key_type, k, p)])}
            if res[k]['total'] > 0:
                for t in ['rating','distance']:
                    res[k][t] = {'sum' : sum([ p.get(t,0) for p in places if matches(key_type, k, p)])}
                    res[k][t]['average'] = res[k][t]['sum'] / res[k]['total']
                    res[k][t]['min'] = min([ p.get(t,0) for p in places if matches(key_type, k, p)])
                    res[k][t]['max'] = max([ p.get(t,0) for p in places if matches(key_type, k, p)])
        results[key_type + '-stats'] = res
    return results

def not_closed(node):
    return (not bool(node['is_closed']))

def matches(key_type, key, node):
    #print('check match - type {}, key {}, node {}'.format(key_type,key,node))
    if key_type == 'price':
        return (key == node.get('price',False)) and not_closed(node)
    elif key_type == 'rating':
        return key == round(node['rating']) and not_closed(node)
    elif key_type == 'rating-no-round':
        return key == node['rating'] and not_closed(node)
    elif key_type == 'distance':
        return (key == node['distance']) and not_closed(node)
    elif key_type == 'distance-group':
        return (key == round(node['distance'] / quarter_mile)) and not_closed(node)
    else:
        return key in [i['alias'] for i in node['categories']] and not_closed(node)

def get_list(key_type, r):
    #print('get_list - type {}, node {}'.format(key_type,r))
    if key_type == 'categories':
        return [i['alias'] for i in r['categories']]
    elif key_type == 'rating':
        return [round(r.get('rating',0))]
    elif key_type == 'distance-group':
        return [round(r.get('distance',0) / quarter_mile)]
    else:
        return [r.get(key_type,None)]

def unique_flatten(t):
    unique_list = []
    for sublist in t:
        for item in sublist:
            if item not in unique_list:
                unique_list.append(item)
    return unique_list

def geocode(address, api, batch=False, config=None):
    if api == 'mapquest':
        params = {  'key' : mapquest_key,
                    'location' : address,
                    'maxResults' : 1
                 }
        req = requests.get(url=mapquest_url, params=params)
        req.raise_for_status()
        location = json.loads(req.text)
        #print('Location {}'.format(json.dumps(location, indent=2)))
        if not batch:
            geo = ( location['results'][0]['locations'][0]['latLng']['lat'],
                    location['results'][0]['locations'][0]['latLng']['lng'])
            return {'formatted_address':'', 'geo' : geo}
        else:
            locations = []
            for r in location['results']:
                locations.append({
                    'formated_address':r['providedLocation']['location'],
                    'geo': (r['locations'][0]['latLng']['lat'], r['locations'][0]['latLng']['lng'])
                })
            return locations
    elif api == 'google':
        address_detail = gmaps.geocode(address=address)
        if len(address_detail) == 0:
            print('No results found? address {}, result {}'.format(address, address_detail))
            return None
        else:
            formatted_address = address_detail[0]['formatted_address']
            geo = address_detail[0]['geometry']['location']
            geo = (geo['lat'],geo['lng'])
            return { 'formatted_address' : formatted_address, 'geo' : geo }
    else:
        cls = get_geocoder_for_service(api)
        geolocator = cls(**config)
        location = geolocator.geocode(address)
        return {
            'formatted_address' : location.address,
            'geo' : (location.latitude, location.longitude)
        }
