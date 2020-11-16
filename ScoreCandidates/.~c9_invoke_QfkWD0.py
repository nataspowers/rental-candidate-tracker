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

joe_ranking = {
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
    'attributes': 1
}

property_type = {
    'townhouse': 7,
    'apartment': 2,
    'single family home': 10,
    'single family home single family house': 10,
    'condo': 8,
    'multi family': 4
}

positive_attributes_scores = {
    "It's walkable to grocery stores": 5,
    "It's dog friendly": 5,
    "It's walkable to restaurants": 5,
    "Neighbors are friendly": 2,
    "People would walk alone at night": 8,
    "Streets are well-lit": 5,
    "Parking is easy": 5,
    "There are sidewalks": 5,
    "It's quiet": 5,
    "Yards are well-kept": 1,
    "There's wildlife" : 5,
    "There's holiday spirit": 0,
    "They plan to stay for at least 5 years": 1
}

negative_attributes_scores = {
    "Car is needed": 5,
    "There are community events": 1,
    "Kids play outside": 1
}

types = ['area','bedrooms','bathrooms','pets','rent','deposit','ppsf','year_built','fitness','ac','days_on_market']

lower_better_types = ['rent','year_built','deposit', 'ppsf','days_on_market']

boolean_types = ['pets','ac','fitness']

missing_property_types = []
missing_attributes = []


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
        item['score'] = score_item(item, stats)
        item['score']['total'] = reduce(lambda a,b : a+b, item['score'].values())
        #print('Item {} scores {}'.format(item['Address'], json.dumps(item['score'], indent=2, cls=DecimalEncoder)))

    sorted_items = sorted(items, key = lambda i: i['score']['total'], reverse=True)
    max_score = reduce(lambda a,b : a+b, joe_ranking.values()) * 10

    for i in range(0,10):
        score = sorted_items[i]['score']['total']
        pct = (score / max_score) * 100
        print('#{} {} score {} ({}%)'.format(i+1, sorted_items[i]['Address'], score, pct))


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
        stat['min'] = 100000
    if 'max' not in stat:
        stat['max'] = 0

    if val is not None:
        stat['count'] += 1
        stat['total'] += val
        stat['max'] = val if val > stat['max'] else stat['max']
        stat['min'] = val if val < stat['min'] else stat['min']
    return stat

def generate_averages(items):
    for key in items:
        if key in boolean_types:
            items[key]['avg'] = 0.5
        else:
            items[key]['avg'] = items[key]['total'] / items[key]['count']
    return items

def score_item(item, stats):
    s = {}
    for key, rank in joe_ranking.items():
        if key == 'property_type':
            s[key] = property_type.get(item['details'][key], 5) * rank
        elif key == 'attributes':
            s[key] = score_attributes(item['details'].get(key,[])) * rank
        else:
            missing_value = stats[key]['avg']
            val = get_val(key,item)
            reverse = key in lower_better_types
            s[key] = score_normal(val, missing_value, rank, stats[key]['max'], stats[key]['min'], reverse=reverse)
    return s

def score_attributes(attrs):
    score = 0
    max_attribute = reduce(lambda a,b : a+b, positive_attributes_scores.values())
    for k, v in attrs.items():
        if k in positive_attributes_scores:
            score += positive_attributes_scores[k] * (v / 100)
        if k in negative_attributes_scores:
            score -= negative_attributes_scores[k] * (v / 100)
    # todo - map on a range


def map_range(value, old_max, old_min, new_max, new_min):
    old_span = old_max - old_min
    new_span = new_max - new_min

    old_span = old_span if old_span != 0 else 1

    scaled = float(value - old_min) / float(old_span)
    return new_min + (scaled * new_span)

def score_normal(value, missing_value, rank, old_max, old_min, reverse=False):
    val = value if bool(value) else missing_value
    #print('Calculating score using {} from range ({},{}) to range (0,10) with rank {}'.format(val, old_min, old_max, rank))
    #print('unranked score {}'.format(map_range(val,old_max,old_min,10,0)))
    score = map_range(val,old_max,old_min,10,0)
    if reverse:
        return score * rank
    else:
        return (10 - score) * rank

def get_val(key, item):
    #print('Getting value {} from {}'.format(key, json.dumps(item, indent=2, cls=DecimalEncoder)))
    if key == 'ppsf':
        if 'area' in item['details'] and 'rent' in item['price']:
            val =  get_val('rent', item) / get_val('area', item)
        else:
            val = None
        if val is not None and val > 3:
            print('found {} with ppsf over 3 = {}'.format(item['Address'], val))
    elif key == 'area':
        val = item['details'].get('area',None)
        if val is not None and val > 10000:
            print('found {} with area over 10000 = {}'.format(item['Address'], val))
            val = val / 10
    elif key == 'bedrooms':
        val = item['details'].get('bedrooms',None)
    elif key == 'days_on_market':
        val = item['details'].get('days_on_market',None)
    elif key == 'bathrooms':
        val = item['details'].get('bathrooms',None)
    elif key == 'rent':
        val = item['price'].get('rent',None)
    elif key == 'deposit':
        val = item['price'].get('deposit',None)
        val = val if val is not None and val > 1000 else None
    elif key == 'pets':
        val = item['details']['pets']
        val = 1 if val is not None else None
    elif key == 'fitness':
        val = item['details'].get('fitness',False)
        val = 1 if val else None
    elif key == 'ac':
        val = item['details'].get('ac',False)
        val = 1 if val else None
    elif key == 'year_built':
        val = item['details'].get('year_built',False)
        val = 2020 - val if val else None
    else:
        val = None

    #print('Got value {}'.format(val))
    return val

def print_missing_property_types(items):
    for item in items:
        pt = item['details'].get('property_type',None)
        if pt is not None and pt not in property_type and pt not in missing_property_types:
            missing_property_types.append(pt)
    if missing_property_types:
        print ('Missing Property Types: {}'.format(missing_property_types))

def print_missing_attributes(items):
    pas = positive_attributes_scores
    nas = negative_attributes_scores

    for item in items:
        for attr in item['details'].get('attributes',{}):
            for k, v in attr.items():
                if k not in pas and k not in nas and k not in missing_attributes:
                    missing_attributes.append(k)
    if missing_attributes:
        print ('Missing attributes: {}'.format(missing_attributes))

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)