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

def range(value, old_min, old_max, new_min, new_max):
    return new_max - (((value - old_min) * (new_max-new_min)) / (old_max-old_min)) + new_min
