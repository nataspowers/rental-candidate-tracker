import json
import requests
import os

ws_api_key = os.environ['ws_api_key']
ws_url = os.environ['ws_url']
ws_header = os.environ['ws_header']

def get_walk_score(geo, address):
    params = {'lat': geo[0],
              'lon': geo[1],
              'address': address,
              'wsapikey': ws_api_key,
              'transit': 1,
              'bike': 1,
              'format': 'json'
    }
    req = requests.get(url=ws_url, params=params, headers=ws_header)
    scores = json.loads(req.text)
    if scores['status'] == 1:
        return {'walk':scores['walkscore'],
                'transit':scores.get('transit',{}).get('score',None),
                'bike':scores.get('bike',{}).get('score',None)}
    else:
        return None