import json
import boto3
import math
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import collections
from pykml.factory import KML_ElementMaker as KML
from lxml import etree
import inflect

person_list = ['all', 'joe', 'jen']
styles = {}
sorted_candidates = {'both': [], 'joe' : [], 'jen' : []}


def sort(candidates):
    start = datetime.now()

    count = 0
    sorted_candidates['both'] = sorted(candidates, key = lambda i: i.get('score',{}).get('joe',{}).get('pct',-1) + i.get('score',{}).get('jen',{}).get('pct',-1), reverse=True)
    sorted_candidates['joe'] = sorted(candidates, key = lambda i: i.get('score',{}).get('joe',{}).get('total',-1), reverse=True)
    sorted_candidates['jen'] = sorted(candidates, key = lambda i: i.get('score',{}).get('jen',{}).get('total',-1), reverse=True)
    print('Sorted Addresses {} times in {}'.format(count, datetime.now() - start))
    for idx, i in enumerate(sorted_candidates['both']):
        joe_rank = sorted_candidates['joe'].index(i) + 1
        jen_rank = sorted_candidates['jen'].index(i) + 1
        avg_rank = (joe_rank + jen_rank) / 2
        combined_pct = i['score']['joe']['pct'] + i['score']['jen']['pct']
        print('{} {} avg {}, joe {}, jen {}, joe pct {}, jen pct {}, combined pct {}'.format(idx+1, i['Address'], avg_rank, joe_rank, jen_rank,
                                                                                            i['score']['joe']['pct'], i['score']['jen']['pct'], combined_pct))
    return sorted_candidates

def load_all_candidates():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    scan_kwargs = {
        'FilterExpression': Key('status').eq('active')
    }
    done = False
    start_key = None
    items = []

    start = datetime.now()
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        items = items + response.get('Items', [])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    print('Loaded {} candidate addresses in {}'.format(len(items), datetime.now() - start))
    return items

#Creating 6 groups with different colors - each group has 20 items in it
def generate_styles(doc):
    colors = ['ffd18802','ff2f8b55','ff00d6ff','ff1427a5','ff7e231a','ff2f8b55',]
    for idx, color in enumerate(colors):
        styles[idx+1] = []
        for i in range(0,19):
            color_id = '{}{}{}'.format(color[6:8],color[4:6],color[2:4]).capitalize()
            style_normal = KML.Style(
                  KML.IconStyle(
                    KML.scale(1.0),
                    KML.color(color),
                    KML.Icon(
                      KML.href("images/icon-{}.png".format(i+1)),
                    ),
                  ),
                  KML.LabelStyle(
                      KML.scale(0.0)
                  ),
                  KML.BallonStyle(
                      KML.text('FIXME')
                  ),
                  id="icon-seq2-{}-{}-{}-normal".format(i, idx, color_id)
                )
            doc.append(style_normal)
            #styles.append(style_normal.get('id'))

            style_highlight = KML.Style(
                  KML.IconStyle(
                    KML.scale(1.0),
                    KML.color(color),
                    KML.Icon(
                      KML.href("images/icon-{}.png".format(i+1)),
                    ),
                  ),
                  KML.LabelStyle(
                      KML.scale(1.0)
                  ),
                  KML.BallonStyle(
                      KML.text('FIXME')
                  ),
                  id="icon-seq2-{}-{}-{}-highlight".format(i, idx, color_id)
                )
            doc.append(style_highlight)

            style_map = KML.StyleMap(
                KML.Pair(
                    KML.key('normal'),
                    KML.styleUrl('#{}'.format(style_normal.get('id'))),
                ),
                KML.Pair(
                    KML.key('highlight'),
                    KML.styleUrl('#{}'.format(style_highlight.get('id'))),
                ),
                id="icon-seq2-{}-{}-{}".format(i, idx, color_id)
              )
            doc.append(style_map)
            styles[idx+1].append(style_map.get('id'))

    return doc

def generate_kml():
    doc = KML.kml(
        KML.Document(
            KML.name('Bay Area Houses'),
        )
    )
    doc.Document = generate_styles(doc.Document)

    places = []
    max_pct = (sorted_candidates['both'][0]['score']['joe']['pct'] + sorted_candidates['both'][0]['score']['jen']['pct'])/2
    group_index = 0
    group = 1
    inf = inflect.engine()

    for idx, item in enumerate(sorted_candidates['both']):
        joe_pct = item['score']['joe']['pct']
        jen_pct = item['score']['jen']['pct']
        avg_pct = round(((joe_pct + jen_pct) / 2),2)

        if avg_pct < max_pct - 3:
            if group < 6:
                group += 1
                group_index = 0
                max_pct -= 3
            else:
                break
        else:
            group_index += 1

        places.append(
            KML.Placemark(
                KML.name('{}. {} - Avg Pct {}%'.format(idx+1, item['Address'], avg_pct)),
                KML.description('{} ${} Area: {} Visual: {} Street: {} Pets: {} Crime: V {} NV {} M/R: {} Coffee: {} Walk: {} {} {} {} ({}, {})'.format(
                    item.get('url','?'),
                    item.get('price',{}).get('rent','?'),
                    item.get('details',{}).get('area','?'),
                    get_observation(item,'overall-visual'),
                    get_observation(item,'street'),
                    item.get('details',{}).get('pets','?'),
                    round(int(item.get('crime',{}).get('violent',-1))),
                    round(int(item.get('crime',{}).get('non-violent',-1))),
                    item.get('commute',{}).get('friend',{}).get('duration',{}).get('text','?'),
                    item.get('places',{}).get('coffee',{}).get('closest',{}).get('place',{}).get('url','?'),
                    item.get('walk_score',{}).get('walk','?'),
                    item.get('neighborhood',{}).get('name','Neighborhood ?'),
                    item.get('neighborhood',{}).get('url','?'),
                    item.get('places',{}).get('bart',{}).get('place',{}).get('name','Bart ?'),
                    item.get('places',{}).get('bart',{}).get('commute',{}).get('duration',{}).get('text',''),
                    item.get('places',{}).get('bart',{}).get('commute',{}).get('distance',{}).get('text',''),
                    )),
                KML.styleUrl('#{}'.format(styles[group][group_index])),
                KML.Point(KML.coordinates('{},{},0'.format(item['location']['geo'][1], item['location']['geo'][0]))),
            ),
        )
        if group_index == 0 and group > 1:
            fld_name = '{} 3%'.format(inf.ordinal(group-1))
            fld = KML.Folder(KML.name(fld_name))
            for p in places:
                fld.append(p)
            doc.Document.append(fld)
            places = []

    return doc

def person_rank(person, item, candidates):
    person_sort = sorted(candidates, key = lambda i: i.get('score',{}).get(person,{}).get('total',-1), reverse=True)
    return person_sort.index(item) + 1

def get_observation(item, key):
    joe = item.get('observations',{}).get('joe',{}).get(key,None)
    jen = item.get('observations',{}).get('jen',{}).get(key,None)

    if jen and joe and jen != '?' and joe != '?':
        return (int(jen) + int(joe)) / 2
    elif jen and jen != '?':
        return int(jen)
    elif joe and joe != '?':
        return int(joe)
    else:
        return '?'

if __name__ == '__main__':
    candidates = load_all_candidates()
    sort(candidates)
    kml = generate_kml()
    out = etree.tostring(etree.ElementTree(kml))
    print(out.replace(b'FIXME',b'<![CDATA[<h3>$[name]</h3>]]>'))