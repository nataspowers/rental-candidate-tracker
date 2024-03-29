import json
import boto3
from functools import reduce
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime

from constants import *


print('Loading function')

missing_property_types = []
missing_restaurant_cat_types = []
missing_attributes = []
missing_neighborhoods = {}

crime_violent_nonviolent_ratio = 5
no_neighborhood_score = 5


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')
    scan_kwargs = {
            'FilterExpression': Key('status').eq('active')
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

    print('loaded {} candidates in {}'.format(len(items), datetime.now() - start))
    print_missing_property_types(items)
    print_missing_attributes(items)
    print_missing_rest_cats(items)
    print_missing_neighborhoods(items)

    for person, weight in weights.items():

        start = datetime.now()
        stats = generate_stats(items, person)
        #print('{}"s Calculated stats {}'.format(person, json.dumps(stats, indent=2, cls=DecimalEncoder)))
        stats = generate_averages(stats)
        if person == 'all':
            print("{}'s Calculated stats with averages in {} - {}".format(person, datetime.now() - start, json.dumps(stats, indent=2, cls=DecimalEncoder)))

        start = datetime.now()
        for item in items:
            if not person in item:
                item[person] = {}
            item[person]['score'] = score_item(item, weight, stats, person, items)
            item[person]['total'] = sum(i['score'] for i in item[person]['score'].values() if 'score' in i.keys())
            #print('Item {} scores {}'.format(item['Address'], json.dumps(item['score'], indent=2, cls=DecimalEncoder)))
        print('scored items in {}'.format(datetime.now() - start))

    sorted_items = {}
    start = datetime.now()
    for person in weights.keys():
        sorted_items[person] = sorted(items, key = lambda i: i[person]['total'], reverse=True)
    print('sorted items in {}'.format(datetime.now() - start))

    max_score = {i : reduce(lambda a,b : a+b, weights[i].values()) * 10 for i in weights}

    for person in weights.keys():
        print("{}'s list".format(person))
        for i in range(0,10):
            score = sorted_items[person][i][person]['total']
            address = sorted_items[person][i]['Address']
            url = sorted_items[person][i]['url']
            pct = (score / max_score[person]) * 100
            print('#{} {} ({}) score {} ({}%)'.format(i+1, address, url, round(score,2), round(pct,2)))

    response = update_table(items, max_score)

def score_item(item, weight, stats, person, items):
    start = datetime.now()
    s = {}
    for key, w in weight.items():
        if not key in stats:
            print('Missing stat for key {} for item {}'.format(key, item['Address']))
            break
        missing_value = stats[key]['avg'] if key not in missing_okay else 0
        val = get_val(key, item, person)
        reverse = key in lower_better_types
        val_list = []
        [val_list.append(get_val(key, i, person)) for i in items if get_val(key, i, person) is not None]
        val_list.sort(reverse=(not reverse))
        s[key] = {
            'val' : val,
            'score' : score_normal(val, missing_value, w, stats[key]['max'], stats[key]['min'], reverse=reverse),
            'weight' : w,
            'rank' : {
                'rank' : int(val_list.index(val) if val is not None else int(len(val_list)/2)) + 1,
                'count' : stats[key]['count']
            }
        }
        # Person all has no weighting - so saving total metrics there only. Value can differ from person to person for some types (attributes, restaurant categories), so saving value under person
        if person == 'all':
            s[key]['stats'] = {
                'min' : stats[key]['min'],
                'avg' : round(stats[key]['avg']),
                'max' : stats[key]['max']
            }

    print('For {}, scored item {} in {}'.format(person, item['Address'], datetime.now() - start))
    #print('returning score - {}'.format(s))
    return s

def score_normal(value, missing_value, weight, old_max, old_min, reverse=False):
    val = value if bool(value) else missing_value
    #print('Calculating score using {} from range ({},{}) to range (0,10) with weight {}'.format(val, old_min, old_max, weight))
    score = map_range(val, old_max, old_min, 10, 1)
    if reverse:
        return (11 - score) * weight
    else:
        return score * weight

def generate_stats(items, person):
    stats = {}
    for item in items:
        for t in types:
            if t == 'restaurant.categories':
                #restaurant categories scored against a specific ranking
                pc = restaurant_cat_types[person]
                #have to check if categories exist, as the list comprehension will result in a val of 0 instead of None
                if item.get('places',{}).get('restaurant',{}).get('categories-stats',False):
                    stats[t] = get_stat(stats.get(t,{}),
                                sum([get_val('restaurant.category.' + cat, item) * pc[cat]
                                    for cat in list(pc)
                                        if get_val('restaurant.category.' + cat, item) is not None]))
            else:
                #print('Getting stats for {}'.format(t))
                stats[t] = get_stat(stats.get(t,{}), get_val(t, item, person))
                if t in boolean_types:
                    stats[t]['min'] = 0
    return stats

def get_stat(stat, val):
    if 'count' not in stat:
        stat['count'] = 0
    if 'total' not in stat:
        stat['total'] = 0
    if 'min' not in stat:
        stat['min'] = -1
    if 'max' not in stat:
        stat['max'] = 0

    if val is not None:
        stat['count'] += 1
        stat['total'] += float(val)
        stat['max'] = float(val) if val > stat['max'] else stat['max']
        stat['min'] = float(val) if stat['min'] == -1 or val < stat['min'] else stat['min']
    return stat

def generate_averages(items):
    for key in items:
        if key in boolean_types:
            items[key]['avg'] = 0.5
        elif items[key]['count'] > 0:
            items[key]['avg'] = items[key]['total'] / items[key]['count']
        else:
            items[key]['avg'] = 0
    return items

def map_range(value, old_max, old_min, new_max, new_min):
    """print('mapping value {} from range from {} - {} to range {} - {}'.format(
            value, old_min, old_max, new_min, new_max
    ))"""
    old_span = old_max - old_min
    new_span = new_max - new_min

    old_span = old_span if old_span != 0 else 1

    scaled = float(float(value) - old_min) / float(old_span)
    return new_min + (scaled * new_span) if value != 0 else 0

def get_val(key, item, person=None):
    #print('Getting value {}'.format(key))
    val = None
    if item is None:
        print('called get_val with item = None? - key is {} person is {}'.format(key,person))
        return None

    if key == 'ppsf':
        if 'area' in item.get('details',{}) and 'rent' in item.get('price',{}):
            val =  get_val('rent', item) / get_val('area', item)
        #if val is not None and val > 4:
            #print('found {} with ppsf over 3 = {}'.format(item['Address'], val))
    elif key == 'area':
        val = override(item.get('details',{}).get('area',None),item.get('override',{}).get('details',{}).get('area',None))
        if isinstance(val,str) and '-' in val:
            val = int((val.split('-',1)[1]).strip()) # take the larger end if area is a range
        if val is not None and int(val) > 10000:
            #print('found {} with area over 10000 = {}'.format(item['Address'], val))
            val = int(val) / 10
        elif val:
            val = int(val)
    elif key == 'rent':
        val = override(item.get('price',{}).get('rent',None),item.get('override',{}).get('price',{}).get('rent',None))
        if isinstance(val,str) and '-' in val:
            val = int((val.split('-',1)[1]).strip()) # take the larger end if rent is a range
        elif val:
            val = int(val)
    elif key == 'property_type':
        #print('{} get property_type {}'.format(item['Address'],item.get('details',{}).get('property_type',False)))
        pt = override(item.get('details',{}).get('property_type',False),item.get('override',{}).get('details',{}).get('property_type',False))
        if not pt:
            return None
        val = property_type.get(pt, False)
        if not val:
            return None
        return val
    elif key == 'attributes':
        attrs = item.get('details',{}).get('attributes',False)
        val = get_attributes_val(attrs, person) if attrs else None
    elif key == 'deposit':
        val = override(item.get('price',{}).get('deposit',None),item.get('override',{}).get('price',{}).get('deposit',None))
        val = val if val is not None and val > 1000 else None
    elif key == 'year_built':
        val = item.get('details',{}).get('year_built',False)
        val = 2021 - val if val else None
    elif key == 'crime':
        val = get_crime_val(item.get('crime',None))
    elif get_place_type(key) is not None:
        val = get_place_val(key, item.get('places',None))
    elif key in commute_types:
        val = get_commute_val(key, item.get('commute',None))
    elif key in observation_types:
        val = get_observation_val(key, item.get('observations', None), person)
    elif key == 'neighborhood':
        val = get_neighborhood_val(override(item.get('neighborhood',{}).get('name',None),item.get('override',{}).get('neighborhood',{}).get('name',None)))
    else:
        if key in item:
            val = override(item[key], item.get('override',{}).get(key,None))
        else:
            val = walk_item(key, item)

    if key in boolean_types:
        if val is not None:
            val = int(val)

    #print('Got value {}'.format(val))
    return val

def walk_item(key, node):
    val = None
    if isinstance(node,dict):
        #print('in walk_item - looking for key {} in item {}'.format(key, node.get('Address','Not a top level node')))
        if key in node:
            return node[key]
        for k, v in node.items():
            if k != 'score':
                #print('---> going into {}'.format(k))
                val = walk_item(key, v)
                if val is not None:
                    #print('---> returning {} from {}'.format(val, v))
                    return val
    if isinstance(node,list):
        #print('in walk_item - looking for key {} a list'.format(key))
        for item in node:
            val = walk_item(key, item)
            if val is not None:
                #print('---> returning {} from {}'.format(val, item))
                return val
    #print('---> returning {}'.format(val))
    return val

def override(stored, overrided):
    if overrided is None or not overrided:
        return stored
    else:
        return overrided

def print_missing_property_types(items):
    for item in items:
        pt = item.get('details',{}).get('property_type',None)
        if pt is not None and pt not in property_type and pt not in missing_property_types:
            missing_property_types.append(pt)
    if missing_property_types:
        print ('Missing Property Types: {}'.format(missing_property_types))

def print_missing_attributes(items):
    for item in items:
        for k in item.get('details',{}).get('attributes',{}):
            if k not in all_attributes and k not in missing_attributes:
                missing_attributes.append(k)
    if missing_attributes:
        print ('Missing attributes: {}'.format(missing_attributes))

def print_missing_rest_cats(items):
    categories = get_categories(items)
    rest_cat_types = {**restaurant_cat_types['all'],**restaurant_cat_types['joe'],**restaurant_cat_types['jen']}
    for cat in categories:
        if cat not in rest_cat_types and cat not in missing_restaurant_cat_types:
                missing_restaurant_cat_types.append(cat)
    if missing_restaurant_cat_types:
        print ('Missing restaurant categories: {}'.format(missing_restaurant_cat_types))

def print_missing_neighborhoods(items):
    for item in items:
        neighborhood = item.get('neighborhood',{}).get('name',None)
        if neighborhood and neighborhood not in neighborhood_scores and neighborhood not in missing_neighborhoods:
                missing_neighborhoods[neighborhood] = item.get('neighborhood',{}).get('url', '?')
    if missing_neighborhoods:
        print ('Missing neighborhood:')
        for k, v in missing_neighborhoods.items():
            print ('{} {}'.format(k, v))

def get_categories(node):
    if isinstance(node,list):
        places = [i.get('places',{}) for i in node if i is not None and i.get('places',None) is not None]
    elif isinstance(node, dict):
        places = list(node.get('places',{})) if node.get('places',None) is not None else None

    for p in places:
        if p is None:
            print('node has None place - {}'.format(node))

    categories = [list(p.get('restaurant',{}).get('categories-stats',{}))
                        for p in places
                        if p is not None and p.get('restaurant',{}).get('categories-stats',None) is not None]
    return unique_flatten(categories)


def unique_flatten(t):
    unique_list = []
    for sublist in t:
        for item in sublist:
            if item not in unique_list:
                unique_list.append(item)
    return unique_list

def get_attributes_val(attrs, person='all'):
    # we use the percentage that voted for the attribute
    # as a percent of the person picked weight for that attribute
    # - and sum it all up for the item
    if attrs:
        val = 0
        val += sum(attributes_scores[person][k] * (int(attrs[k]) / 100)
                        for k in attrs
                        if k in attributes_scores[person])
        return val
    else:
        return None

def get_crime_val(crime):
    if crime is None or crime.get('violent',None) is None or crime.get('non-violent',None) is None:
        return None

    return (crime['violent']*crime_violent_nonviolent_ratio) + crime['non-violent']

def get_commute_val(commute_type, commute):
    # currently this returns None if there is not a commute node at all, but treats any other missing data as 0
    # should be safe, as we should have all values
    if commute is None:
        return None

    #print('In get_commute_val - key={}, node is {}'.format(commute_type, bool(commute)))

    work = {
        'drive' : commute['work'].get('drive',{}).get('duration',{}).get('value',0),
        'transit' : commute['work'].get('transit',{}).get('duration',{}).get('value',0)
    }

    friend = commute['friend'].get('duration',{}).get('value',0)

    airport_list = ['OAK','SJC','SFO']

    airports = {}
    for airport in airport_list:
        airports[airport] = {
            'drive' : get_airport(airport, commute.get('airports',{}).get('drive',{})),
            'transit' : get_airport(airport, commute.get('airports',{}).get('transit',{}))
        }

    if commute_type == 'work':
        return sum(work.values())
    elif commute_type == 'work.drive':
        return work['drive']
    elif commute_type == 'work.transit':
        return work['transit']
    elif commute_type == 'friend':
        return friend
    else: # returning an airport
        for a in airport_list:
            if a in commute_type:
                if 'drive' in commute_type:
                    return airports[a]['drive']
                elif 'transit' in commute_type:
                    return airports[a]['transit']
                else:
                    return sum(airports[a].values())
        # not a specific airport, so return all airports
        #print('all airports - return is {}'.format(sum([sum(airports[k].values()) for k in airports.keys()])))
        return sum([sum(airports[k].values()) for k in airports.keys()])

    return None

def get_airport(airport,travel):
    if not bool(travel):
        return 0
    for k, v in travel.items():
        if airport in k:
            return float(v.get('duration',{}).get('value',0))
    return 0

def get_observation_val(obs_type, observations, person):
    if observations is None:
        return None

    joe_val = observations.get('joe',{}).get(obs_type,None)
    joe_val = None if joe_val is None or joe_val == '?' else int(joe_val)

    jen_val = observations.get('jen',{}).get(obs_type,None)
    jen_val = None if jen_val is None or jen_val == '?' else int(jen_val)

    if joe_val is None and jen_val is None:
        return None

    if joe_val is None:
        return jen_val

    if jen_val is None:
        return joe_val

    if person == 'all':
        return float(joe_val + jen_val) / 2
    elif person == 'joe':
        return joe_val
    else:
        return jen_val

def get_place_val(place_type, place):
    #print('in get_place_val key={}, place={}'.format(place_type,place))
    if place is None:
        return None

    if get_place_type(place_type) == 'restaurant':
        return get_restaurant_coffee_val(place_type, place.get('restaurant',None), 'restaurant')
    elif get_place_type(place_type) == 'coffee':
        return get_restaurant_coffee_val(place_type, place.get('coffee',None), 'coffee')
    elif get_place_type(place_type) == 'bart':
        return get_bart_val(place_type, place.get('bart',None))
    elif get_place_type(place_type) == 'convenience_store':
        place = place.get('convenience_store',None)
        if not place:
            return None
        else:
            highest_rated = place.get('highest_rated',{}).get('value',None)
            closest =place.get('closest',{}).get('value',None)
            if highest_rated is not None and closest is not None:
                return highest_rated * closest
            elif highest_rated is not None:
                return highest_rated
            elif closest is not None:
                return closest
    return None

def get_place_type(key):
    #print('in get_place_type key={}'.format(key))
    if 'restaurant' in key:
        return 'restaurant'
    elif 'coffee' in key:
        return 'coffee'
    elif 'bart' in key:
        return 'bart'
    elif 'convenience_store' in key:
        return 'convenience_store'
    else:
        return None

def get_restaurant_coffee_val(key, p, place_type):
    #print('in get_restaurant_val key={}'.format(key))
    if p is None:
        return None

    if key == place_type:
        return int(p['total']) * float(p['rating']['average'])
    elif 'stats' in key:
        if 'price' in key:
            stats = p.get('price-stats',False)
            if not stats:
                return None
            elif '$' in key:
                key = key.strip(place_type+'.stats.price.')
                if not stats.get(key,False):
                    return None
                return int(stats[key]['total']) * float(stats[key]['rating']['average'])
        elif 'rating' in key:
            stats = p.get('rating-stats',False)
            if not stats:
                return None
            elif any(char.isdigit() for char in key):
                key = key.strip(place_type+'.stats.rating.')
                val = stats.get(key,{}).get('total',False)
                if not val:
                    return None
                return float(val)
        elif 'distance' in key:
            stats = p.get('distance-group-stats',False)
            if not stats:
                return None
            elif any(char.isdigit() for char in key):
                key = key.strip(place_type+'.stats.distance.')
                if not stats.get(key,False):
                    return None
                return int(stats[key]['total']) * float(stats[key]['rating']['average'])
    elif 'distance' in key:
        if 'distance-group-stats' in p:
            k = place_type+'.stats.distance.'
            return sum(float(get_restaurant_coffee_val(k+str(i),p,place_type)) * (6-i)
                    for i in range(0,5)
                    if get_restaurant_coffee_val(k+str(i),p,place_type) is not None)
        else:
            return float(p['distance']['average'])
    elif 'rating' in key:
        if p.get('rating-group-stats',False):
            k = place_type+'.stats.rating.'
            return sum(float(get_restaurant_coffee_val(k+str(i),p,place_type)) * i
                    for i in range(1,4)
                    if get_restaurant_coffee_val(k+str(i),p,place_type) is not None)
        return float(p['rating']['average'])
    elif 'category' in key:
        key = key.strip(place_type+'.category.')
        stats = p['categories-stats']
        if not stats.get(key,False):
            return None
        return int(stats[key]['total']) * float(stats[key]['rating']['average'])
    else:
        return None

def get_bart_val(key, place):
    if place is None:
        return None

    duration = place.get('commute',{}).get('duration',{}).get('value',None)
    distance = place.get('commute',{}).get('distance',{}).get('value',None)
    rating = place.get('place',{}).get('rating',None)

    if 'duration' in key:
        return duration
    elif 'distance' in key:
        return distance
    elif 'rating' in key:
        return rating
    else:
        if duration is not None and distance is not None:
            return duration * distance
        else:
            return None

def get_neighborhood_val(key):
    if key in neighborhood_scores:
        return neighborhood_scores[key]
    else:
        return no_neighborhood_score

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def update_table (items, max_score):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    start = datetime.now()
    for i in items:

        score = {p: {'score_detail': i[p]['score'],
                     'total': i[p]['total'],
                     'pct': (i[p]['total'] / max_score[p]) * 100}
                for p in weights.keys() }

        response = table.update_item(
                    Key={'Address': i['Address']},
                    UpdateExpression='set score = :val1, scored_at = :dt',
                    ExpressionAttributeValues={
                        ':val1': json.loads(json.dumps(score, cls=DecimalEncoder), parse_float=Decimal),
                        ':dt' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                )
    print('updated {} items in {}'.format(len(items), datetime.now() - start))

    return response

if __name__ == '__main__':
    lambda_handler(None,None)