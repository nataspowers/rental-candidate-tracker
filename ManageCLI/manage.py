import json
import boto3
from decimal import Decimal
from datetime import datetime, timedelta
import re
import webbrowser

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Candidates')

candidates = []
sorted_candidates = {}
context = {
    'person': None,
    'selected': {}
}

def menu():

    while 1:
        cmd = input(">>> ")
        if cmd == 'exit':
            break
        elif re.search('display', cmd, re.IGNORECASE):
            display(cmd.strip('display').strip())
        elif re.search('select', cmd, re.IGNORECASE):
            select(cmd.strip('select').strip())
        else:
            help()


def display(cmd, person=None):
    context['person'] = person
    if cmd and not cmd.isnumeric() and not person:
        if re.search('joe', cmd, re.IGNORECASE):
            display(cmd.strip('joe').strip(), 'joe')
        elif re.search('jen', cmd, re.IGNORECASE):
            display(cmd.strip('jen').strip(), 'jen')
    elif cmd.isnumeric():
        if person:
            display_person(person,int(cmd))
        else:
            for p in ['joe','jen']:
                display_person(p,int(cmd))
    elif person:
        display_person(person,10)
    else:
        for p in ['joe','jen']:
            display_person(p,10)


def display_person(person:str, number:int):
    print("{}'s top {} list:".format(person, number))
    for n in range(0,number):
        item = sorted_candidates[person][n]
        score = item.get('score',{}).get(person,{}).get('total',-1)
        pct = item.get('score',{}).get(person,{}).get('pct',0)
        address = item['Address']
        print('#{}: {}({}%) {}'.format(n+1, score, pct, address))

def select(cmd):
    if context['person']:
        person = context['person']
        if cmd.isnumeric():
            context['selected'] = sorted_candidates[person][int(cmd)]
            print('selected {}'.format(context['selected']['Address']))
            selected_menu()
        else:
            print("Enter a number to select a specific address from {}'s list - use display {} to display the list".format(person, person))
    else:
        print('You need to select a person to select an address - use display <person> to select a person')

def selected_menu(item=None, prompt=None):

    if not item:
        selected = context['selected']
        prompt = "[{}] ".format(selected['Address'])
        print_item('', selected)
    else:
        selected = item['value']
        prompt += "[{}] ".format(item['name'])
        print_item(item['name'], selected)

    while 1:
        cmd = input(prompt)
        if cmd == 'exit':
            break
        if cmd == 'web':
            launch_menu()
        elif cmd in selected.keys():
            if isinstance(selected[cmd], dict) and has_sub_level(selected[cmd]):
                selected_menu({'name': cmd, 'value':selected[cmd]}, prompt.strip())
            else:
                print(json.dumps(selected[cmd], indent=2, cls=DecimalEncoder))
        else:
            print_item('', selected)

def print_item(name, item):
    display = ''
    for key, value in item.items():
        if not isinstance(value, dict) and not isinstance(value, list):
            display += ' {}: {} |'.format(key, value)
    if display:
        print('{} -> {}'.format(name, display.strip()))

    keys = ''
    for key, value in item.items():
        if isinstance(value, dict):
            keys += '{}'.format(', ' + key if keys else key)
    if keys:
        print('The following keys are available: {}'.format(keys))

def has_sub_level(items):
    if not items:
        return False
    for item in items.values():
        if isinstance(item, dict):
            return True
    return False

def launch_menu():
    if context['selected']:
        webbrowser.open(context['selected']['url'])
    else:
        print('You need to select an address first')

def sort():
    start = datetime.now()
    for person in ['joe','jen']:
        sorted_candidates[person] = sorted(candidates, key = lambda i: i.get('score',{}).get(person,{}).get('total',-1), reverse=True)
    print('Sorted {} candidate addresses in {}'.format((len(sorted_candidates) * len(candidates)), datetime.now() - start))

def load_all_candidates ():

    from boto3.dynamodb.conditions import Key

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

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


if __name__ == '__main__':
    #load_oakland_crime()
    candidates = load_all_candidates()
    sort()
    menu()