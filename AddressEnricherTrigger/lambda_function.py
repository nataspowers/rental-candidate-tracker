import json
from datetime import datetime, timedelta
from crime import get_crime
from util import get_distance, get_next_weekday
from locations import *
from commute import *
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
            address = record['dynamodb']['Keys']['Address']['S']

        if record['eventName'] == 'MODIFY':
            if not 'friend_drive' in record['dynamodb']['NewImage']:
                address = record['dynamodb']['NewImage']['Address']['S']

        if address != None:
            sources.append(address)

    #drive_times = get_drive_time_friend(sources)
    #print('Drive to Friend {}'.format(json.dumps(drive_times, indent=2)))

    #transit_commute = get_commute_transit(sources)
    #print('Commute Transit {}'.format(json.dumps(transit_commute, indent=2)))
    #drive_commute = get_commute_drive(sources)
    #print('Commute Drive {}'.format(json.dumps(drive_commute, indent=2)))

    #airport_transit_commute = get_airport_commute_transit(sources)
    #print('Airport Commute Transit {}'.format(json.dumps(airport_transit_commute, indent=2)))
    #airport_drive_commute = get_airport_commute_drive(sources)
    #print('Airport Commute Drive {}'.format(json.dumps(airport_drive_commute, indent=2)))


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

        #restaurant = get_restaurants(geo)
        #print('Restaurants {}'.format(json.dumps(restaurant, indent=2)))

        #stores = get_convenience_store(geo)
        #print('Convenience Stores {}'.format(json.dumps(stores, indent=2)))

        #bart = get_bart(geo)
        #print('Bart {}'.format(json.dumps(bart, indent=2)))
        #bart_geo = (bart['place']['geometry']['location']['lat'],
        #                bart['place']['geometry']['location']['lng'])
        #bart_commute = get_walking_time(geo, bart_geo)
        #print('Bart Commute {}'.format(json.dumps(bart_commute, indent=2)))
        #bart['commute'] = bart_commute

        #friend_commute = fetch_drive_time(formatted_address, drive_times)[0]

        #commute = {'transit' : fetch_drive_time(formatted_address, transit_commute)[0],
        #            'drive' : fetch_drive_time(formatted_address, drive_commute)[0]}
        #airports = {'transit' : get_airport_commute(formatted_address, airport_transit_commute),
        #            'drive' : get_airport_commute(formatted_address, airport_drive_commute)}
        #print('Airport Commute {}'.format(json.dumps(airports, indent=2)))
        #places = {'coffee' : coffee, 'restaurant' : restaurant,
        #            'convenience_store':stores, 'bart':bart}

        #res = update_table(address, friend_commute, commute, places, scores, crime)

    return 'Successfully processed {} records.'.format(len(event['Records']))

def get_airport_commute(address, commute):
    commutes = {}
    for idx, airport in enumerate(commute['destination_addresses']):
        commutes[airport] = fetch_drive_time(address, commute)[idx]
    return commutes

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