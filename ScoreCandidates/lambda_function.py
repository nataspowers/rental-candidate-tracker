import json
import boto3
from functools import reduce
from boto3.dynamodb.conditions import Key
from decimal import Decimal


print('Loading function')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Candidates')
scan_kwargs = {
        'FilterExpression': Key('status').eq('active')
}

weights = {
    'joe' : {
        'ppsf' : 10,
        'rent' : 5,
        'area' : 5,
        'deposit' : 2,
        'year_built' : 4,
        'pets': 2,
        'property_type': 6,
        'ac': 8,
        'fitness': 3,
        'days_on_market': 2,
        'bedrooms': 5,
        'bathrooms': 5,
        'attributes': 1,
        'crime': 8,
        'bart' : 8,
        'kitchen' : 8,
        'livingroom' : 5
    },
    'jen':{
        'area': 1
    }
}

property_type = {
    'townhouse': 7,
    'apartment': 2,
    'single family home': 10,
    'single family home single family house': 10,
    'condo': 8,
    'multi family': 4,
}

places_types = ['coffee','restaurant','convenience_store','bart']

observation_types = ['visual','kitchen','bath','livingroom']

commute_types = ['work','friend','airports','work.drive','work.transit','airports.OAK',
                 'airports.SJC','airports.SFO','airports.drive','airports.transit',
                 'airports.OAK.drive','airports.OAK.transit','airports.SJC.drive',
                 'airports.SJC.transit','airports.SFO.drive','airports.SFO.transit']

attributes_scores = {
    "It's walkable to grocery stores": 4,
    "It's dog friendly": 7,
    "It's walkable to restaurants": 8,
    "Neighbors are friendly": 2,
    "People would walk alone at night": 8,
    "Streets are well-lit": 6,
    "Parking is easy": 7,
    "There are sidewalks": 5,
    "It's quiet": 5,
    "Yards are well-kept": 1,
    "There's wildlife" : 3,
    "There's holiday spirit": 0,
    "They plan to stay for at least 5 years": 0,
    "Car is needed": -5,
    "There are community events": -1,
    "Kids play outside": -1,
}

# These generate stats, even if they aren't used in ranking
types = ['area','bedrooms','bathrooms','pets','rent','deposit','ppsf','year_built',
         'fitness','ac','days_on_market','attributes','crime','walk_score']
types += commute_types
types += places_types
types += observation_types

lower_better_types = ['rent','year_built','deposit', 'ppsf','days_on_market','crime']
lower_better_types += commute_types

boolean_types = ['pets','ac','fitness']

missing_property_types = []
missing_attributes = []

crime_violent_nonviolent_ratio = 5


def lambda_handler(event, context):
    done = False
    start_key = None
    items = []
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        items = items + response.get('Items', [])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    stats = generate_stats(items)
    #print('Calculated stats {}'.format(json.dumps(stats, indent=2, cls=DecimalEncoder)))
    print('Missing property types {}'.format(json.dumps(missing_property_types, indent=2, cls=DecimalEncoder)))
    stats = generate_averages(stats)
    print('Calculated stats with averages {}'.format(json.dumps(stats, indent=2, cls=DecimalEncoder)))

    print_missing_property_types(items)
    print_missing_attributes(items)

    for item in items:
        for person, weight in weights.items():
            if not item.get(person,False):
                item[person] = {}
            item[person]['score'] = score_item(item, weight, stats)
            item[person]['total'] = reduce(lambda a,b : a+b, item[person]['score'].values())
            #print('Item {} scores {}'.format(item['Address'], json.dumps(item['score'], indent=2, cls=DecimalEncoder)))

    sorted_items = {}
    for person in weights.keys():
        sorted_items[person] = sorted(items, key = lambda i: i[person]['total'], reverse=True)

    max_score = {i : reduce(lambda a,b : a+b, weights[i].values()) * 10 for i in weights}

    for person in weights.keys():
        print("{}'s list".format(person))
        for i in range(0,10):
            score = sorted_items[person][i][person]['total']
            address = sorted_items[person][i]['Address']
            url = sorted_items[person][i]['url']
            pct = (score / max_score[person]) * 100
            print('#{} {} ({}) score {} ({}%)'.format(i+1, address, url, score, pct))

def score_item(item, weight, stats):
    s = {}
    for key, rank in weight.items():
        if key == 'property_type':
            s[key] = property_type.get(item.get('details',{}).get(key,''), 5) * rank
        else:
            missing_value = stats[key]['avg']
            val = get_val(key,item)
            reverse = key in lower_better_types
            s[key] = score_normal(val, missing_value, rank, stats[key]['max'], stats[key]['min'], reverse=reverse)
    return s

def score_normal(value, missing_value, rank, old_max, old_min, reverse=False):
    val = value if bool(value) else missing_value
    #print('Calculating score using {} from range ({},{}) to range (0,10) with rank {}'.format(val, old_min, old_max, rank))
    #print('unranked score {}'.format(map_range(val,old_max,old_min,10,0)))
    score = map_range(val,old_max,old_min,10,0)
    if reverse:
        return score * rank
    else:
        return (10 - score) * rank

def generate_stats(items):
    stats = {}
    for item in items:
        for t in types:
            stats[t] = get_stat(stats.get(t,{}), get_val(t, item))
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
        stat['total'] += val
        stat['max'] = val if val > stat['max'] else stat['max']
        stat['min'] = val if stat['min'] == -1 or val < stat['min'] else stat['min']
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
    old_span = old_max - old_min
    new_span = new_max - new_min

    old_span = old_span if old_span != 0 else 1

    scaled = float(value - old_min) / float(old_span)
    return new_min + (scaled * new_span)

def get_val(key, item):
    #print('Getting value {} from {}'.format(key, json.dumps(item, indent=2, cls=DecimalEncoder)))
    val = None
    if key == 'ppsf':
        if 'area' in item.get('details',{}) and 'rent' in item.get('price',{}):
            val =  get_val('rent', item) / get_val('area', item)
        if val is not None and val > 3:
            print('found {} with ppsf over 3 = {}'.format(item['Address'], val))
    elif key == 'area':
        val = item.get('details',{}).get('area',None)
        if val is not None and val > 10000:
            print('found {} with area over 10000 = {}'.format(item['Address'], val))
            val = val / 10
    elif key == 'attributes':
        attrs = item.get('details',{}).get('attributes',False)
        val = get_attributes_val(attrs) if attrs else None
    elif key == 'deposit':
        val = item.get('price',{}).get('deposit',None)
        val = val if val is not None and val > 1000 else None
    elif key == 'year_built':
        val = item.get('details',{}).get('year_built',False)
        val = 2020 - val if val else None
    elif key == 'crime':
        val = get_crime_val(item.get('crime',None))
    elif key in places_types:
        val = get_place_val(key, item.get('places',{}).get(key,None))
    elif key in commute_types:
        val = get_commute_val(key, item.get('commute',None))
    elif key == 'walk_score':
        val = item.get(key, {}).get('walk', None)
    elif key in observation_types:
        val = get_observation_val(key, item.get('observations', None))
    else:
        if key in item:
            val = item[key]
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
        if key in node:
            return node[key]
        for v in node.values():
            val = walk_item(key, v)
            if val is not None:
                return val
    if isinstance(node,list):
        for item in node:
            val = walk_item(key, item)
            if val is not None:
                return val
    return val

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
            if k not in attributes_scores and k not in missing_attributes:
                missing_attributes.append(k)
    if missing_attributes:
        print ('Missing attributes: {}'.format(missing_attributes))

def get_attributes_val(attrs):
    if attrs:
        val = 0
        for k in attrs:
            if k in attributes_scores:
                val += attributes_scores[k] * (int(attrs[k]) / 100)
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

    work = {
        'drive' : commute['work'].get('drive',{}).get('duration',{}).get('value',0),
        'transit' : commute['work'].get('transit',{}).get('duration',{}).get('value',0)
    }

    friend = commute['friend'].get('transit',{}).get('duration',{}).get('value',0)

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
        return sum([sum(airports[k].values()) for k in airports.keys()])

    return None

def get_airport(airport,travel):
    if bool(travel):
        return 0
    for k, v in travel.items:
        if airport in k:
            return v.get('duration',{}).get('value',0)
    return 0

def get_observation_val(obs_type, observations):
    if observations is None:
        return None

    joe = observations.get('joe',None)
    jen = observations.get('jen',None)

    if joe is None and jen is None:
        return None

    if joe is None:
        return jen[obs_type]

    if jen is None:
        return joe[obs_type]

    return joe[obs_type] + jen[obs_type] / 2

def get_place_val(place_type, place):
    return None

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)