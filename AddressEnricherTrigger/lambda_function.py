import json
from datetime import datetime, timedelta
from crime import get_crime
from util import get_distance, get_next_weekday
from locations import get_coffee_shops, get_convenience_store, get_restaurants, geocode
from commute import get_drive_time_friend, get_commute_drive, get_commute_transit, fetch_drive_time
from walkscore import get_walk_score
import boto3

print('Loading function')

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
        address_detail = geocode(address=address, api="google")
        #print(json.dumps(address_detail, indent=2))
        formatted_address = address_detail['formatted_address']
        geo = address_detail['geo']

        #scores = get_walk_score(geo, formatted_address)
        #print('Scores {}'.format(json.dumps(scores, indent=2)))

        #crime = get_crime(geo)
        #print('Crime {}'.format(json.dumps(crime, indent=2)))

        #coffee = get_coffee_shops(geo)
        #print('Coffee {}'.format(json.dumps(coffee, indent=2)))

        restaurant = get_restaurants(geo)
        print('Restaurants {}'.format(json.dumps(restaurant, indent=2)))

        #stores = get_convenience_store(geo)
        #print('Convenience Stores {}'.format(json.dumps(stores, indent=2)))

        #friend_commute = fetch_drive_time(formatted_address, drive_times)
        #commute = {'transit' : fetch_drive_time(formatted_address, transit_commute),
        #            'drive' : fetch_drive_time(formatted_address, drive_commute)}
        #places = {'coffee' : coffee, 'restaurant' : restaurant, 'convenience_store':stores}

        #res = update_table(address, friend_commute, commute, places, scores, crime)

    return 'Successfully processed {} records.'.format(len(event['Records']))

def update_table (key, friend_drive, commute, places, score, crime):
    print('updating table {} - friend_drive = {}, commute = {}, places = {}, walk_score = {}, crime = {}'
            .format(key, friend_drive, commute, places, score, crime))
    #update_expr = 'set friend_drive = :val1, set commute = :val2, places = :val3, walk_score = :val4, crime = :val5'
    #response = table.update_item(
    #            Key={'address': key},
    #            UpdateExpression=update_expr,
    #            ExpressionAttributeValues={
    #               ':val1': friend_drive,
    #               ':val2': commute,
    #               ':val3': places,
    #               ':val4': score,
    #               ':val5': crime
    #            }
    #       )
    #return response
    return ''