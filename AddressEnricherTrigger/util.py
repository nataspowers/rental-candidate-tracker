from datetime import datetime, timedelta
from geopy import distance


def get_distance(coords_1, coords_2):
    #print('start {} - end {}'.format(coords_1,coords_2))
    return distance.distance(coords_1, coords_2).miles

def get_next_weekday(startdate, weekday):
    """
    @startdate: given date, in format '2013-05-25'
    @weekday: week day as a integer, between 0 (Monday) to 6 (Sunday)
    """
    d = datetime.strptime(startdate, '%Y-%m-%d')
    t = timedelta((7 + weekday - d.weekday()) % 7)
    return (d + t).strftime('%Y-%m-%d')

def map_range(value, old_min, old_max, new_min, new_max):
    old_span = old_max - old_min
    new_span = new_max - new_min

    old_span = old_span if old_span != 0 else 1

    scaled = float(value - old_min) / float(old_span)
    return new_min + (scaled * new_span)
