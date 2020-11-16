import json
import boto3
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
    'rent' : 9,
    'deposit' : 8
}


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
    print('Calculated stats {}'.format(json.dumps(stats, indent=2)))
    stats = generate_averages(stats)
    print('Calculated stats with averages {}'.format(json.dumps(stats, indent=2)))
    for item in items:
        item['score'] = score_item(item, stats)
        print('Item score {}'.format(json.dumps(item['score'], indent=2)))


def generate_stats(items):
    stats = {}
    for item in items:
        stats['area'] = get_stat(stats.get('area',{}), get_val('area', item))
        stats['bedrooms'] = get_stat(stats.get('bedrooms',{}), get_val('bedrooms', item))
        stats['bathrooms'] = get_stat(stats.get('bathrooms',{}), get_val('bathrooms', item))
        stats['pets'] = get_stat(stats.get('pets',{}), get_val('pets', item))
        stats['rent'] = get_stat(stats.get('rent',{}), get_val('rent', item))
        stats['deposit'] = get_stat(stats.get('deposit',{}), get_val('deposit', item))
        stats['ppsf'] = get_stat(stats.get('ppsf',{}), get_val('ppsf', item))

    return stats

def get_stat(stat, val):
    if 'count' not in stat:
        stat['count'] = 0
    if 'total' not in stat:
        stat['total'] = 0
    if 'min' not in stat:
        stat['min'] = 0
    if 'max' not in stat:
        stat['max'] = 0

    stat['count'] += 1 if val > 0 else 0
    stat['total'] += val
    stat['max'] = val if val > stat['max'] else stat['max']
    stat['min'] = val if val < stat['min'] else stat['min']
    return stat

def generate_averages(items):
    for key in items:
        items[key]['avg'] = items[key]['total'] / items[key]['count']
    return items

def score_item(item, stats):
    s = {}
    for key, rank in joe_ranking.items():
        missing_value = stats[key]['avg']
        val = get_val(key,item)
        s[key] = score(val, missing_value, rank, stats[key]['max'], stats[key]['min'])
    return s


def map_range(value, old_max, old_min, new_max, new_min):
    return (((value - old_min) * (new_max-new_min)) / (old_max-old_min)) + new_min

def score(value, missing_value, rank, old_max, old_min):
    val = value if bool(value) else missing_value
    print('Calculating score using {} from range ({},{}) to range (0,10) with rank {}'
                .format(val, old_min, old_max, rank))
    return map_range(val,old_max,old_min,0,10) * rank

def get_val(key, item):
    print('Getting value {} from {}'.format(key, json.dumps(item, indent=2, cls=DecimalEncoder)))
    if key == 'ppsf':
        if 'area' in item['details'] and 'rent' in item['price']:
            val = item['details']['area'] / item['price']['rent']
        else:
            val = 0
    elif key == 'area':
        val = item['details'].get('area',0)
    elif key == 'bedrooms':
        val = item['details'].get('bedrooms',0)
    elif key == 'bathrooms':
        val = item['details'].get('bathrooms',0)
    elif key == 'deposit':
        val = item['price'].get('deposit',0)
        val = val if val > 100 else 0
    elif key == 'pets':
        val = 1 if item['details'].get('pets',None) else 0

    print('Got value {}'.format(val))
    return val

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return (str(o) for o in [o])
        return super(DecimalEncoder, self).default(o)