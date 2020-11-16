import googlemaps
import requests
import json
import os

from util import get_distance

gmaps = googlemaps.Client(key=os.environ['gmap_key'])

yelp_url = os.environ['yelp_url']
yelp_header = {'Authorization': 'Bearer %s' % os.environ['yelp_api_key']}
dinner_categories = os.environ['dinner_categories'].split(",")
lunch_categories = os.environ['lunch_categories'].split(",")
brunch_categories = os.environ['brunch_categories'].split(",")

mapquest_key = os.environ['mapquest_key']
mapquest_url = os.environ['mapquest_url']

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
    catagories = ','.join(str(s) for s in (dinner_categories + lunch_categories + brunch_categories))
    params = {'latitude':start[0],
              'longitude':start[1],
              'radius':805,
              'categories':catagories,
              'sort_by':'distance',
              'limit' : 50
    }

    restauraunts = {"businesses" : []}
    received, total = -1, 0
    while received < total and received < 1000:
        req = requests.get(url=yelp_url, params=params, headers=yelp_header)
        response = json.loads(req.text)
        restauraunts['businesses'] += response['businesses']
        total = response['total']
        received += len(response['businesses'])
        params['offset'] = received

        #print(json.dumps(response, indent=2))
    results = filter_places('yelp',restauraunts, start, 3.5)
    results['total'] = total + 1
    results['ratings'] = {'sum' : sum([ r.get('rating',0) for r in restauraunts['businesses'] ])}
    results['ratings']['average'] = results['ratings']['sum'] / results['total']
    results['distances'] = {'sum' : sum([ r.get('distance',0) for r in restauraunts['businesses'] ])}
    results['distances']['average'] = results['distances']['sum'] / results['total']

    pri = {}
    cat = {}
    rat = {}
    for r in restauraunts['businesses']:

        price = r.get('price',False)
        if price:
            if not pri.get(price,False):
                pri[price] = {'count' : 0, 'rating' : 0, 'distance' : 0}
            pri[price]['count'] += 1
            pri[price]['rating'] += r['rating']
            pri[price]['distance'] += r['distance']

        rating = round(r['rating'])
        if not rat.get(rating,False):
            rat[rating] = {'count' : 0, 'distance' : 0}
        rat[rating]['count'] += 1
        rat[rating]['distance'] += r['distance']

        for rest_cat in r['categories']:
            alias = rest_cat['alias']
            if not cat.get(alias,False):
               cat[alias] = {'count' : 0, 'rating' : 0, 'distance' : 0}
            cat[alias]['count'] += 1
            cat[alias]['rating'] += r['rating']
            cat[alias]['distance'] += r['distance']

    results['price-stats'] = pri
    results['category-stats'] = cat
    results['rating-stats'] = rat

    print(json.dumps(results, indent = 2))

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

def get_bart(start):

    bart = gmaps.places(query='bart',
                    location=start,
                    radius=5000,
                    type='transit_station')

    results = filter_places('google', bart, start, 0.0)
    #print(json.dumps(results, indent=2))
    return results['closest']

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
    return {'highest_rated':{'place':highest_rated,'value':highest_rated.get('rating',0)},
            'closest':{'place':closest,'value':closest_dist}}

def geocode(address, api):
    if api == 'mapquest':
        params = {  'key':mapquest_key,
                    'location':address
                 }
        req = requests.get(url=mapquest_url, params=params)
        location = json.loads(req.text)
        #print('Location {}'.format(json.dumps(location, indent=2)))
        geo = ( location['results'][0]['locations'][0]['latLng']['lat'],
                location['results'][0]['locations'][0]['latLng']['lng'])
        return {'formatted_address':'', 'geo' : geo}
    else:
        address_detail = gmaps.geocode(address=address)
        formatted_address = address_detail[0]['formatted_address']
        geo = address_detail[0]['geometry']['location']
        geo = (geo['lat'],geo['lng'])
        return {'formatted_address':formatted_address, 'geo' : geo}