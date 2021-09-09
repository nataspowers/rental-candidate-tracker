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



def get_drive_time_friend(start):
    """
    @start: location to calculate driving distance from
    """
    now = datetime.now()
    weekend = get_next_weekday(now.strftime("%Y-%m-%d"), 5) + " 14:00:00"
    weekend = datetime.strptime(weekend, '%Y-%m-%d %H:%M:%S')

    #print('Getting dinstances from these locations {}'.format(start))
    distances = get_distance_matrix(origins=start,
                                        destinations=friend_geo,
                                        mode="driving",
                                        units="imperial",
                                        departure_time=weekend)

    print('distance matrix to friend complete in {}'.format(datetime.now() - now))
    return distances

def get_commute_transit(start):
    """
    @start: location to calculate transit distance from
    """
    now = datetime.now()
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 08:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = get_distance_matrix(origins=start,
                                    destinations=work_geo,
                                    mode="transit",
                                    units="imperial",
                                    transit_routing_preference="fewer_transfers",
                                    arrival_time=monday_morning)

    print('distance matrix to work/transit complete in {}'.format(datetime.now() - now))
    return distance

def get_commute_drive(start):
    """
    @start: location to calculate driving distance from
    """
    now = datetime.now()
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 07:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = get_distance_matrix(origins=start,
                                    destinations=work_geo,
                                    mode="driving",
                                    units="imperial",
                                    departure_time=monday_morning)

    print('distance matrix to work/drive complete in {}'.format(datetime.now() - now))
    return distance

def get_walking_time(start, destination):
    """
    @start: location to calculate walking distance from
    """
    now = datetime.now()
    print('Getting walking distance matrix from {} to {}'.format(start, destination))
    distance = get_distance_matrix(origins=start,
                                    destinations=destination,
                                    mode="walking",
                                    units="imperial")

    print('distance matrix walking complete in {}'.format(datetime.now() - now))
    return distance

def get_airport_commute_drive(start):
    """
    @start: location to calculate driving distance from
    """
    now = datetime.now()
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 08:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = get_distance_matrix(origins=start,
                                    destinations=airports,
                                    mode="driving",
                                    units="imperial",
                                    departure_time=monday_morning)

    print('distance matrix airport/drive complete in {}'.format(datetime.now() - now))
    return distance

def get_airport_commute_transit(start):
    """
    @start: location to calculate transit distance from
    """
    now = datetime.now()
    monday_morning = get_next_weekday(now.strftime("%Y-%m-%d"), 0) + " 09:00:00"
    monday_morning = datetime.strptime(monday_morning, '%Y-%m-%d %H:%M:%S')
    distance = get_distance_matrix(origins=start,
                                    destinations=airports,
                                    mode="transit",
                                    units="imperial",
                                    transit_routing_preference="fewer_transfers",
                                    arrival_time=monday_morning)
    print('distance matrix airport/transit complete in {}'.format(datetime.now() - now))
    return distance

def get_distance_matrix(origins, destinations, mode, units, departure_time=None, arrival_time=None,
                        transit_routing_preference=None):
    #setup output dict
    distances = {
        'destination_addresses': [],
        'origin_addresses': [],
        'rows': []
    }

    #do we have a list of destinations, or just one destination - an destination can be a geo (list, tuple or dict), or a string address
    if all(isinstance(el, list) for el in destinations) or \
       all(isinstance(el, tuple) for el in destinations) or \
       all(isinstance(el, dict) for el in destinations) or \
       all(isinstance(el, str) for el in destinations):
        dest_count = len(destinations)
    else:
        dest_count = 1

    max_batch = min(25,100/dest_count) #25 origins or destinations, max 100 returned items total
    #print('{} destination{}, max batch set to {}'.format(dest_count, 's' if dest_count > 1 else '', max_batch))

    #do we have a list of origins, or just one origin - an origin can be a geo (list, tuple or dict), or a string address
    if all(isinstance(el, list) for el in origins) or \
       all(isinstance(el, tuple) for el in origins) or \
       all(isinstance(el, dict) for el in origins) or \
       all(isinstance(el, str) for el in origins):
        origin_count = len(origins)
    else:
        origin_count = 1

    #print('{} origin{}'.format(origin_count, 's' if origin_count > 1 else ''))

    sls = 0
    while sls < origin_count:
        if origin_count > 1:
            sle = min(sls+max_batch, origin_count+1)
            batch = origins[sls:sle]
            #print('slice {} to {} for batch of {} - origins={}'.format(sls,sle,len(batch),len(origins)))
        else:
            sle = 1
            batch = origins

        #print('calling distance matrix with origin batch {} - destination batch {}'.format(batch,destinations))
        dm = gmaps.distance_matrix(origins=batch,
                                    destinations=destinations,
                                    mode=mode,
                                    units=units,
                                    departure_time=departure_time,
                                    arrival_time=arrival_time,
                                    transit_routing_preference=transit_routing_preference)
        distances['destination_addresses'] = dm['destination_addresses']
        distances['origin_addresses'].extend(dm['origin_addresses'])
        distances['rows'].extend(dm['rows'])
        sls = sle
    return distances


def fetch_drive_time(item_to_find, matrix, index_list):
    #print('looking for: {}'.format(item_to_find))
    #print('In Matrix {}'.format(json.dumps(matrix, indent=2)))
    index = index_list.index(item_to_find)
    #print('index found = {}'.format(index))
    #print('Matrix {}'.format(json.dumps(matrix['rows'], indent=2)))
    return matrix['rows'][index]['elements']