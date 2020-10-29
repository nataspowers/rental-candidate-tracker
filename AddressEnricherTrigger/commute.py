import googlemaps
import os
import json
from datetime import datetime

from util import get_distance, get_next_weekday

friend_geo = tuple(os.environ['friend'].split(","))
work_geo = tuple(os.environ['work'].split(","))
airport_list = tuple(os.environ['airports'].split(";"))
airports = []
for airport in airport_list:
    airports.append(tuple(airport.split(',')))

gmaps = googlemaps.Client(key=os.environ['gmap_key'])

now = datetime.now()

def get_drive_time_friend(start):
    """
    @start: location to calculate driving distance from
    """

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
    @start: location to calculate transit distance from
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

def get_walking_time(start, destination):
    """
    @start: location to calculate walking distance from
    """

    distance = gmaps.distance_matrix(origins=start,
                                    destinations=destination,
                                    mode="walking",
                                    units="imperial")
    return distance

def get_airport_commute_drive(start):
    """
    @start: location to calculate driving distance from
    """
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 08:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = gmaps.distance_matrix(origins=start,
                                    destinations=airports,
                                    mode="driving",
                                    units="imperial",
                                    departure_time=monday_morning)
    return distance

def get_airport_commute_transit(start):
    """
    @start: location to calculate transit distance from
    """
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 09:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = gmaps.distance_matrix(origins=start,
                                    destinations=airports,
                                    mode="transit",
                                    units="imperial",
                                    transit_routing_preference="fewer_transfers",
                                    arrival_time=monday_morning)
    return distance


def fetch_drive_time(item_to_find, matrix):
    #print('looking for: {}'.format(item_to_find))
    #print('In Matrix {}'.format(json.dumps(matrix, indent=2)))
    index = matrix['origin_addresses'].index(item_to_find)
    #print('index found = {}'.format(index))
    #print('Matrix {}'.format(json.dumps(matrix['rows'], indent=2)))
    return matrix['rows'][index]['elements']