import json
import boto3
import curses
import math
from curses import wrapper
from curses import ascii
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import re
import textwrap
import collections

candidates = []
sorted_candidates = {}
context = {
    'person': 'all',
    'sort_key': 'score',
    'section_selected' : 'sort_keys',
    'selected': {}
}
person_list = ['all', 'joe', 'jen']

select_address_sections = ['person','sort_keys','addresses']


def curses_init(stdscr):
    stdscr.clear()
    curses.curs_set(False)

    stdscr.addstr(0, 0, "Loading...", curses.A_REVERSE)
    stdscr.refresh()
    stdscr.move(1,0)
    candidates = load_all_candidates(stdscr)

    stdscr.addstr(2,0, "Sorting...", curses.A_REVERSE)
    stdscr.refresh()
    stdscr.move(3,0)
    sort(stdscr, candidates)

    curses.napms(1000)
    stdscr.clear()

    stdscr.move(0,0)
    while 1:
        select_addres_menu(stdscr)  # we will exit the program from this menu if q or ESC is pressed
        if context['selected']:
            display_address_menu(stdscr)
    curses.endwin()

def select_addres_menu(window):

    display_select_screen(window)

    while True:
        c = window.getch()
        if c == ord('q') or c == curses.ascii.ESC:
            exit()  # Exit the while loop
        elif c == ord('!'):
            context['person'] = 'all'
            context['section_selected'] = 'sort_keys'
        elif c == ord('@'):
            context['person'] = 'joe'
            context['section_selected'] = 'sort_keys'
        elif c == ord('#'):
            context['person'] = 'jen'
            context['section_selected'] = 'sort_keys'
        elif c == ord('\t'):
            idx = select_address_sections.index(context['section_selected'])
            idx = idx + 1 if idx < len(select_address_sections)-1 else 0
            context['section_selected'] = select_address_sections[idx]
        elif curses.ascii.isdigit(c):
            if context['section_selected'] == 'person':
                if c == ord('1'):
                    context['person'] = 'all'
                if c == ord('2'):
                    context['person'] = 'joe'
                if c == ord('3'):
                    context['person'] = 'jen'
                context['section_selected'] = 'sort_keys'
            elif context['section_selected'] == 'sort_keys':
                keys = list(sorted_candidates[context['person']].keys())
                context['sort_key'] = keys[int(chr(c))]
            elif context['section_selected'] == 'addresses':
                idx = int(chr(c))
                idx = idx - 1 if idx > 0 else 0 # display index starts at 1 - if the user presses 1 we want to select index 0
                context['selected'] = sorted_candidates[context['person']][context['sort_key']][idx]
        elif c == curses.KEY_DOWN:
            if context['section_selected'] == 'sort_keys':
                keys = list(sorted_candidates[context['person']].keys())
                idx = keys.index(context['sort_key'])
                idx = idx + 1 if idx < len(keys)-1 else 0
                context['sort_key'] = keys[idx]
            if context['section_selected'] == 'addresses':
                if not context['selected']:
                    context['selected'] = sorted_candidates[context['person']][context['sort_key']][0]
                keys = [i['Address'] for i in sorted_candidates[context['person']][context['sort_key']]]
                idx = keys.index(context['selected']['Address'])
                idx = idx + 1 if idx < len(keys)-1 else 0
                context['selected'] = sorted_candidates[context['person']][context['sort_key']][idx]
        elif c == curses.KEY_UP:
            if context['section_selected'] == 'sort_keys':
                keys = list(sorted_candidates[context['person']].keys())
                idx = keys.index(context['sort_key'])
                idx = idx - 1 if idx > 0 else len(keys)
                context['sort_key'] = keys[idx]
            if context['section_selected'] == 'addresses':
                if not context['selected']:
                    context['selected'] = sorted_candidates[context['person']][context['sort_key']][0]
                keys = [i['Address'] for i in sorted_candidates[context['person']][context['sort_key']]]
                idx = keys.index(context['selected']['Address'])
                idx = idx - 1 if idx > 0 else len(keys)
                context['selected'] = sorted_candidates[context['person']][context['sort_key']][idx]
        elif c == ord('\n'):
            if context['section_selected'] == 'sort_keys':
                context['section_selected'] = 'addresses'
            elif context['section_selected'] == 'addresses':
                break
        display_select_screen(window)


def display_select_screen(window):
    person = context['person'] if context['person'] else 'all'
    sort_key = context['sort_key'] if context['sort_key'] else 'score'

    window.clear()
    window.move(0, 0)

    if context['section_selected'] == 'person':
        window.standout()
    else:
        window.standend()
    display_person_select(person, window)
    top, x = window.getyx()
    if context['section_selected'] == 'sort_keys':
        window.standout()
    else:
        window.standend()
    x = display_sort_keys(person, sort_key, window)
    left_edge = x + 2
    window.move(top, left_edge)
    if context['section_selected'] == 'addresses':
        window.standout()
    else:
        window.standend()
    display_list(person, sort_key, window)
    window.refresh()
    window.standend()

def display_person_select(person, window):
    for idx, p in enumerate(person_list):
        s = 'SHIFT + {} | {}'.format(idx+1, p)
        f = curses.A_REVERSE if p == person else curses.A_NORMAL
        window.addstr(s, f)
        window.addstr(' |')

    draw_horizontal_line(window)

def display_sort_keys(person, selected, window):
    rows, cols = window.getmaxyx()
    max_key = max([len(k) for k in sorted_candidates[person].keys()])
    max_idx = len(str(len(sorted_candidates[person].keys())))
    max_column_width = max_idx + max_key + 3
    top, x = window.getyx()

    column = 0
    for idx, key in enumerate(sorted_candidates[person]):
        f = curses.A_REVERSE if selected == key else curses.A_NORMAL
        idx_padding = ' ' * (max_idx - len(str(idx)))
        key_padding = ' ' * (max_key - len(key))
        try:
            window.addstr('{}{}| {}'.format(idx, idx_padding, key), f)
            window.addstr('{}|'.format(key_padding))
            y, x = window.getyx()
            if y < rows-1: # not at the bottom of the screen yet
                window.move(y + 1, column)
            else:
                window.move(top, x)
                column = x
        except Exception as e:
            curses.endwin()
            print('{} {} {}'.format(y, x, e))
            exit()
    return x # x is the end of the last string written, before the cursor was moved


def display_list(person, sort_key, window):
    rows, cols = window.getmaxyx()
    y, x = window.getyx()

    for idx, item in enumerate(sorted_candidates[person][sort_key]):
        f = curses.A_REVERSE if item == context['selected'] else curses.A_NORMAL
        if y < rows - 1: #not sure why this needs to be -1
            score = item.get('score',{}).get(person,{}).get('total',-1)
            pct = item.get('score',{}).get(person,{}).get('pct',0)
            address = item['Address']
            person_obsv_flag = '[NR]' if len(item.get('observations',{}).get(person,{})) == 0 else ''

            # if the current person has not rated and the other person has, flag it with what thier score rank is
            not_person = 'joe' if person == 'jen' else 'jen'
            not_person = 'all' if person == 'all' else not_person
            not_person_obsv_flag = '[{} - {}]'.format(not_person, int(sorted_candidates[not_person][sort_key].index(item)) + 1) \
                if len(item.get('observations',{}).get(not_person,{})) != 0  \
                    and len(item.get('observations',{}).get(person,{})) == 0 else ''

            try:
                removed = '[REMOVED]' if item['status'] == 'removed' else ''
                window.addstr(y, x, '{}'.format(removed), curses.A_REVERSE)
                window.addstr(y, x + len(removed), '#{}: {} - {}({}%)'.format(idx+1, address, score, pct),f)
                if person_obsv_flag:
                    window.addstr(' ', curses.A_NORMAL)
                    window.addstr('{}'.format(person_obsv_flag), curses.A_REVERSE)
                if not_person_obsv_flag:
                    window.addstr(' ', curses.A_NORMAL)
                    window.addstr('{}'.format(not_person_obsv_flag), curses.A_REVERSE)
            except curses.error:
                curses.endwin()
                print(' In display_list - Attempt to print at {}, {} - current window max {}, {}'.format(y, x, rows, cols))
            y += 1
        else:
            break

def display_address_menu(window):
    window.clear()
    window.move(0, 0)
    window.box()
    window.refresh()
    screen = window.subwin(1,1)
    display_address_screen(screen)


    while True:
        c = window.getch()

        if c == ord('q') or c == curses.ascii.ESC:
            break  # Exit the while loop
        elif c == curses.KEY_DOWN:
            keys = [i['Address'] for i in sorted_candidates[context['person']][context['sort_key']]]
            idx = keys.index(context['selected']['Address'])
            idx = idx + 1 if idx < len(keys)-1 else 0
            context['selected'] = sorted_candidates[context['person']][context['sort_key']][idx]
        elif c == curses.KEY_UP:
            keys = [i['Address'] for i in sorted_candidates[context['person']][context['sort_key']]]
            idx = keys.index(context['selected']['Address'])
            idx = idx - 1 if idx > 0 else len(keys)-1
            context['selected'] = sorted_candidates[context['person']][context['sort_key']][idx]
        elif c == ord('s'):
            display_scores_modal(screen)
        elif c == ord('o'):
            display_observation_input_modal(screen)
            screen.refresh()
        elif c == ord('r'):
            remove_item()
            screen.refresh()

        display_address_screen(screen)

def display_address_screen(window):

    window.clear()
    window.move(0,0)

    person = context['person']
    sort_key = context['sort_key']
    item = context['selected']
    try:
        window.addstr('Person Selected {} | Sort Key {} | Item # {}'.format(person, sort_key, int(sorted_candidates[person][sort_key].index(item)) + 1))
        window.addstr(' | Score {} '.format(round_item(item['score'][person]['total'], n=2)))
        window.addstr('({}%)'.format(round_item(item['score'][person]['pct'],n=2)))
        for key, value in item.get('observations',{}).get(person,{}).items():
            window.addstr('| {} {} '.format(key, value))

        draw_horizontal_line(window)

        display_address_details(window)
        top, x = window.getyx()
        #top += 1
        window.move(top, 0)
        y, score_left_edge = display_scores(window, item.get('score',{}).get(person,{}).get('score_detail',{}))
        left_edge = score_left_edge
        window.move(top, left_edge)
        y, x = display_description(window, item.get('details',{}).get('description'))
        top += y
        window.move(top, left_edge)
        y, x = display_attributes(window, item.get('details',{}).get('attributes',{}))
        left_edge += x
        window.move(top, left_edge)
        y, x = display_commute(window, item.get('commute',{}), item.get('places',{}).get('bart'))
        top += y
        left_edge = score_left_edge
        window.move(top, left_edge)
        y, x = display_places(window, item.get('places',{}))
        top += y
        window.move(top, left_edge)
        y, x = display_coffee(window, item.get('places',{}).get('coffee',{}))
        top += y
    except:
        curses.endwin()
        y, x = window.getyx()
        print("Current cursor ({},{}) - address {}, sort_key {}, person {}".format(y, x, item['Address'], sort_key, person))
    #window.move(top, left_edge)
    window.refresh()

def display_address_details(window):
    item = context['selected']

    removed = '[REMOVED]' if item['status'] == 'removed' else ''
    window.addstr('{}'.format(removed), curses.A_REVERSE)

    window.addstr('{}'.format(item['Address']))
    window.addstr(' {}'.format(item.get('url','{Missing}')))
    window.addstr(' | {}'.format(item.get('details',{}).get('telephone','{Missing}')))
    draw_horizontal_line(window)
    window.addstr('{}'.format(item.get('neighborhood',{}).get('name','{Unknown Neighborhood}')))
    window.addstr(' {}'.format(item.get('neighborhood',{}).get('url','')))
    window.addstr(' | Days on Market: {}'.format(item.get('details',{}).get('days_on_market','{Missing}')))
    draw_horizontal_line(window)
    window.addstr('{}'.format(item.get('details',{}).get('property_type','{Unknown Property Type}')))
    window.addstr(' built in {}'.format(item.get('details',{}).get('year_built','{Unknown}')))
    window.addstr(' | Area SQFT {}'.format(item.get('details',{}).get('area','{Missing}')))
    window.addstr(' | {} Bed{}'.format(item.get('details',{}).get('bedrooms','{Missing}'),'' if int(item.get('details',{}).get('bedrooms',0)) == 1 else 's'))
    window.addstr(' | {} Bath{}'.format(item.get('details',{}).get('bathrooms','{Missing}'),'' if int(item.get('details',{}).get('bathrooms',0)) == 1 else 's'))
    window.addstr(' |')
    window.addstr(' Walk {} '.format(item.get('walk_score',{}).get('walk','{Missing}')))
    window.addstr(' Transit {} '.format(item.get('walk_score',{}).get('transit','{Missing}')))
    window.addstr(' Bike {} '.format(item.get('walk_score',{}).get('bike','{Missing}')))
    draw_horizontal_line(window)
    window.addstr('Rent {}'.format(item.get('price',{}).get('rent','{Missing}')))
    window.addstr(' (PPSF {})'.format(round_item(item.get('score',{}).get('all',{}).get('score_detail',{}).get('ppsf',{}).get('val'),n=3)))
    window.addstr(' | Deposit {}'.format(item.get('price',{}).get('deposit','{Missing}')))
    window.addstr(' | Pets {}'.format(boolean_display(item.get('details',{}).get('pets'))))
    window.addstr(' | AC {}'.format(boolean_display(item.get('details',{}).get('ac'))))
    window.addstr(' | Heating {}'.format(boolean_display(item.get('details',{}).get('heating'))))
    window.addstr(' | Fitness {}'.format(boolean_display(item.get('details',{}).get('fitness'))))
    window.addstr(' | Crime:')
    window.addstr(' Violent {}'.format(round_item(item.get('crime',{}).get('violent'))))
    window.addstr(' / Non-Violent {}'.format(round_item(item.get('crime',{}).get('non-violent'))))
    draw_horizontal_line(window)

    features = item.get('details',{}).get('features',[])
    ignored_features = ['property type','year built','air conditioning', 'fitness center','deposit','days on market','allowed']
    if (len(features) > 0):
        rows, cols = window.getmaxyx()
        for i in item.get('details',{}).get('features',[]):
            if all(substring not in i.lower() for substring in ignored_features):
                y, x = window.getyx()
                if x + len(i) + 3 >= cols: # +3 for the spaces and seperator
                    x = 0
                    window.move(y+1, x)
                window.addstr('{}{}'.format('' if x == 0 else ' | ', i))
        draw_horizontal_line(window)


def display_scores(window, scores, inverted=False):

    #Use an OrderedDict to sort the dict by key
    scores = collections.OrderedDict(sorted(scores.items()))
    # display table takes a dict, where each dict entry is a column. The key for the dict is the header, and the values are a list
    s = {
        'Key' : [str(i) for i in scores.keys()],
        'Val' : [str(round_item(i,n=2,missing='')) for i in (s['val'] for s in scores.values())],
        'S' : [str(round_item(i,n=2,missing='')) for i in (s['score'] for s in scores.values())],
        'W' : ['{} '.format(round_item(i,n=2,missing='')) for i in (s['weight'] for s in scores.values())],
        'Rank' : ['{} of {}'.format(i['r'], i['c']) for i in yield_ranks(scores)],
    }

    if context['person'] == 'all':
        s['Stat'] = ['{}/{}/{}'.format( round_item(i['min'],n=2,missing=' '),
                                        round_item(i['avg'],n=2,missing=' '),
                                        round_item(i['max'],n=2,missing=' ')) for i in yield_stats(scores)]


    return display_table(window, s, curses.A_REVERSE if inverted else curses.A_NORMAL)

def display_description(window, desc_list):
    rows, cols = window.getmaxyx()
    y, x = window.getyx()

    desc_list = split_lines(desc_list, cols-x-2) #2 for border

    max_width = min(max([len(i) for i in desc_list]), cols - x - 2)

    max_length = min(len(desc_list), rows - y - 2) # +2 is the header and line

    screen = window.derwin(max_length+2, max_width+2, y,x) # +2 for the border
    screen.box()
    panel = screen.derwin(1,1) # creating a new subwindow inside the previous window, so we don't worry about writing over the border
    panel_rows, panel_cols = panel.getmaxyx()

    for l in desc_list:
        if l:
            panel.addstr(l)

            y, x = panel.getyx()
            if y < panel_rows-2: # dont move the cursor if we are already at the bottom
                panel.move(y+1, 0)
            else:
                break # stop writing scores if we hi t the bottom

    panel.refresh()
    return screen.getmaxyx()

def display_commute(window, commute, bart):

    c = reorganize_commute(commute, bart)

    # so that we can display walk and drive in the same column, we are doing a custom merge of those lists
    # this works because bart has walk and no drive, and everything else has drive and no walk
    # if we had both, the first (walk) would be used
    s = {
        ' ' : [str(i) for i in c.keys()],
        'W/D' : merge([v.get('walk',{}).get('duration','') for v in c.values()],
                      [v.get('drive',{}).get('duration','') for v in c.values()]),
        'mi' : merge([v.get('walk',{}).get('distance','') for v in c.values()],
                     [v.get('drive',{}).get('distance','') for v in c.values()]),
        'Transit' : [v.get('transit',{}).get('duration','') for v in c.values()],
        't mi' : [v.get('transit',{}).get('distance','') for v in c.values()],
    }

    return display_table(window, s, curses.A_NORMAL)

def display_places(window, p):
    # bart is covered under commute
    bart = p.pop('bart', None)
    s = {
        ' ' : [str(i) for i in p.keys()],
        'by rating' : [v.get('highest_rated',{}).get('place',{}).get('name','') if v else '' for v in p.values()],
        'r' : [str(v.get('highest_rated',{}).get('value','')) for v in p.values()],
        'miles' : [str(round(convert_miles(v.get('highest_rated',{}).get('place',{}).get('distance', 0)), 2)) for v in p.values()],
        'by distance' : [v.get('closest',{}).get('place',{}).get('name','') for v in p.values()],
        'mi' : [str(round(convert_miles(v.get('closest',{}).get('value', 0)), 2)) for v in p.values()],
        'rating' : [str(v.get('closest',{}).get('place',{}).get('rating','')) for v in p.values()],
    }

    p['bart'] = bart #push it back on, so it doesn't disapear forever more

    return display_table(window, s, curses.A_NORMAL)

def display_coffee(window, c):

    c_flat = collections.OrderedDict(sorted(flatten_places_stats(c).items()))

    s = {
        'coffee ({})'.format(c.get('total','NA')) :[str(i) for i in c_flat.keys() ],
        't' : [str(round_item(v.get('total'),n=2,missing='')) for k, v in c_flat.items() ],
        'avg' : [str(round_item(v.get('average'),n=2,missing='')) for k, v in c_flat.items() ],
        'min' : [str(round_item(v.get('min'),n=2,missing='')) for k, v in c_flat.items() ],
        'max' : [str(round_item(v.get('max'),n=2,missing='')) for k, v in c_flat.items() ],
        'sum' : [str(round_item(v.get('sum'),n=2,missing='')) for k, v in c_flat.items()],
    }

    return display_table(window, s, curses.A_NORMAL)

def display_attributes(window, p):

    s = {
        ' ' : [str(i) for i in p.keys()],
        '%' : [str(i) for i in p.values()],
    }

    return display_table(window, s, curses.A_NORMAL)

def display_table(window, items, color):
    listOfLengths = [len(i) for i in items.values()]
    if not items or listOfLengths.count(0) == len(listOfLengths): # all columns have 0 values
        return 0, 0

    rows, cols = window.getmaxyx()
    y, x = window.getyx()

    # this is a list, indexed by column number, of the max length of each column
    cols_max = [max(len(str(key)), max([len(str(i)) for i in value])) for key, value in items.items()]

    max_width = sum(cols_max) + len(cols_max) - 1 # len(cols_max) -1 is the number of column dividers - one less than the number of columns
    max_width = max_width if max_width <= cols - x - 2 else cols - x - 2

    max_length = max([len(v) for v in items.values()]) + 2 # +2 for the header and the divider
    max_length = max_length if max_length <= rows - y - 2 else rows - y - 2

    screen = window.derwin(max_length+2, max_width+2, y, x) # +2 for the border
    screen.box()
    panel = screen.derwin(1,1) # creating a new subwindow inside the previous window, so we don't worry about writing over the border
    panel_rows, panel_cols = panel.getmaxyx()

    idx = 0
    cur_col = 0
    for header, column in items.items():

        # the three values are the value, a number of spaces, and a divider. No divider on the last column, as the box is there already
        panel.addstr('{}{}{}'.format(header, ' ' * (int(cols_max[idx]) - len(header)), '|' if idx < len(cols_max) - 1 else ''), color)

        y, col_end = panel.getyx()
        if y < panel_rows - 3:
            if idx == 0: #draw the horizontal line after we do the first column - after that, just skip over it
                draw_horizontal_line(panel) #this leaves the cursor in the first column after the line
            else:
                panel.move(y+2, cur_col)
        else:
            break

        for val in column:
            panel.addstr('{}{}{}'.format(val, ' ' * (int(cols_max[idx]) - len(val)), '|' if idx < len(cols_max) - 1 else ''), color)

            y, x = panel.getyx()
            if y < panel_rows - 2:
                panel.move(y+1, cur_col)
            else:
                break

        panel.move(0, col_end)
        cur_col = col_end # col_end is the x position after we print the header for the column - should be at the end of that col
        idx += 1

    panel.refresh()
    return screen.getmaxyx()


def display_scores_modal(window):
    rows, cols = window.getmaxyx()
    item = context['selected']
    person = context['person']

    screen = curses.newwin(rows - 1, cols - 1, 1, 1)
    screen.box()
    panel = screen.derwin(1,1)

    display_scores(panel, item.get('score').get(person).get('score_detail'), inverted=False)
    panel.refresh()

    c = window.getch()

    del screen

def display_observation_input_modal(window):
    item = context['selected']
    person = context['person']

    observations = {
        'Observations' : [
            'overall-visual', 'kitchen', 'livingroom', 'bath', 'floors', 'laundry', 'parking', 'street', 'bonus'
            ]
    }

    items = sum([len(i) for i in observations.values()])
    item_max_length = max([len(i) for i in [item for sublist in observations.values() for item in sublist]]) + 3 # +3 is space, colon, space
    headers = len(observations)
    border = 1
    prompt = 1
    spacer = 2

    r = (border * 2) + items + (headers * 2) + (spacer * 2)
    c = (border * 2) + item_max_length + prompt + (spacer * 2)

    screen = curses.newwin(r, c, 10, 50)
    screen.box()
    screen.refresh()
    panel = screen.derwin(border + spacer, border + spacer)

    y, x = panel.getyx()
    prompts = []
    item_obvs = item.get('observations',{}).get(person,{})

    for k, v in observations.items():
        panel.addstr(y, x, k)
        draw_horizontal_line(panel)
        for idx, i in enumerate(v):
            row_pos = idx + 2
            prompt = (row_pos, x + len(i) + 3)
            prompts.append(prompt)
            try:
                panel.addstr(row_pos, x, '{} : '.format(i))
                panel.addstr(row_pos, prompt[1], str(item_obvs.get(i,'_')))
            except:
                curses.endwin()
                y, x = panel.getyx()
                print("Current cursor ({},{}) - writing {} or {}, idx {}".format(y, x, i, item_obvs.get(i,'_'), idx))

    #turn on displaying the cursor, and echoing input to the screen
    curses.curs_set(1)
    curses.echo()

    #set the cursor to the first input spot, load existing values (if any) and move the cursor there
    selected = 0
    obv = {idx : val for idx, val in enumerate(item_obvs.values())}
    move_prompt(panel, old=0, new=0, prompts=prompts, values=obv)

    update = True
    while True:
        panel.touchwin()
        c = panel.getch()

        if c == ord('q') or c == curses.ascii.ESC:
            update = False
            break  # Exit the while loop
        elif c == curses.KEY_DOWN:
            old_selected = selected
            selected = selected + 1 if selected < items - 1 else 0
            move_prompt(panel, old_selected, selected, prompts, obv)
        elif c == curses.KEY_UP:
            old_selected = selected
            selected = selected - 1 if selected > 0 else items - 1
            move_prompt(panel, old_selected, selected, prompts, obv)
        elif c == curses.KEY_ENTER or c == ord('\n') or c == ord('\r'):
            break
        else:
            old_selected = selected
            selected = selected + 1 if selected < items - 1 else 0
            obv[old_selected] = chr(c)
            move_prompt(panel, old_selected, selected, prompts, obv)

    if update:
        save_observations(observations['Observations'], obv)

    curses.curs_set(0)
    curses.noecho()
    del screen

def move_prompt(screen, old, new, prompts, values):
    # This method will write over the old cursor location the value input (if there was one, else _), replace the new input spot with a highlighted area
    # (including an existing value, if there is one, else ' '), and then move the cursor to that new location

    #curses.endwin()
    #print('moving cursor to {} from {} with values {}'.format(prompts[new], prompts[old], values))

    screen.addstr(prompts[old][0], prompts[old][1], str(values.get(old, '_')))
    screen.addstr(prompts[new][0], prompts[new][1], str(values.get(new, ' ')), curses.A_REVERSE)
    screen.move(prompts[new][0], prompts[new][1])
    screen.refresh()

def save_observations(obv_labels, obv_values):
    observations = {}
    for idx, label in enumerate(obv_labels):
        observations[label] = obv_values[idx]

    #curses.endwin()
    #print('Saving observations {}'.format(observations))
    person = context['person']

    #first - update the in memory already sorted lists so we aren't reloading and resorting
    for person_dict in sorted_candidates.values():
        for key_list in person_dict.values():
            for item in key_list:
                if item['Address'] == context['selected']['Address']:
                    if 'observations' not in item:
                        item['observations'] = {person : observations}
                    else:
                        item['observations'][person] = observations

    #next, update the currently selected item (just in case)
    if 'observations' in context['selected']:
        context['selected']['observations'][person] = observations
    else:
        context['selected']['observations'] = {person : observations}

    #lastly, update dynamoDB - might want to populate all of these somewhere and do it once if performance is an issue - but when?
    session = boto3.session.Session()
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    response = table.update_item(
        Key={'Address': context['selected']['Address']},
        UpdateExpression='set observations = :val1, obv_updated = :dt',
        ExpressionAttributeValues = {
            ':val1': context['selected']['observations'],
            ':dt' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    )

def remove_item():
    #curses.endwin()
    #print('Removing item {}'.format(context['selected']['Address']))
    person = context['person']

    #first - update the in memory already sorted lists so we aren't reloading and resorting
    for person_dict in sorted_candidates.values():
        for key_list in person_dict.values():
            for item in key_list:
                if item['Address'] == context['selected']['Address']:
                    item['status'] = 'removed'

    #next, update the currently selected item (just in case)
    context['selected']['status'] = 'removed'

    #lastly, update dynamoDB - might want to populate all of these somewhere and do it once if performance is an issue - but when?
    session = boto3.session.Session()
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('Candidates')

    response = table.update_item(
        Key={'Address': context['selected']['Address']},
        UpdateExpression='set #s = :val1, updated = :dt',
        ExpressionAttributeValues = {
            ':val1': 'removed',
            ':dt' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        ExpressionAttributeNames={
            '#s':'status'
        },
    )


def draw_horizontal_line(window):
    y, x = window.getyx()
    rows, cols = window.getmaxyx()
    if y + 1 < rows:            # if we are already on the last line, we can't draw
        for x in range(0, cols-1):
            window.addstr(y+1, x, '-')
        if y + 2 < rows:        # if we drew a line on the last line of the window, then we can't move
            window.move(y+2, 0)

def round_item(s, n=0, missing='{Missing}'):
    # return no digits after the decimal if it's a whole number, or would round to a whole number
    if type(s) in [Decimal,str,float]:
        if float(s).is_integer():
            return int(s)
        elif float(round(float(s),n)).is_integer():
            return int(s)
        else:
            return round(float(s),n)
    elif isinstance(s,int):
        return s
    else:
        return missing

def boolean_display(s):
    if s is None or s == 'Null':
        return 'Unknown'
    elif s:
        return 'Yes'
    elif not s:
        return 'No'

def split_lines(lines, length):
    if isinstance(lines,list):
        #textwrap returns a list, so a list comprehension with it retrns a list of lists
        return [item for sublist in [textwrap.wrap(l,width=length) for l in lines] for item in sublist]
    elif isinstance(lines,str):
        return textwrap.wrap(lines, width=length)

def yield_ranks(items):
    for item in items.values():
        yield {
                'r' : item['rank']['rank'],
                'c' : item['rank']['count']
        }

def yield_stats(items):
    for item in items.values():
        yield {
                'min' : item['stats']['min'],
                'avg' : item['stats']['avg'],
                'max' : item['stats']['max']
        }




def sort(window, candidates):
    start = datetime.now()

    count = 0
    for p in person_list:
        count += 1
        sorted_candidates[p] = {'score' : sorted(candidates, key = lambda i: i.get('score',{}).get(p,{}).get('total',-1), reverse=True)}
        types = [item for sublist in [list(i['score'][p]['score_detail'].keys()) for i in candidates] for item in sublist]
        count += len(types)
        for t in types:
            sorted_candidates[p][t] = sorted(candidates, key = lambda i: i.get('score',{}).get(p,{}).get('score_detail',{}).get(t,{}).get('score',-1), reverse=True)
    window.addstr('Sorte Addresses {} times in {}'.format(count, datetime.now() - start))
    window.refresh()

def load_all_candidates (window):
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

    window.addstr('Loaded {} candidate addresses in {}'.format(len(items), datetime.now() - start))
    window.refresh()
    return items


def reorganize_commute(commute, bart):
    sfo = 'San Francisco International Airport (SFO), San Francisco, CA 94128, USA'
    sjc = 'Norman Y. Mineta San Jose International Airport (SJC), 1701 Airport Blvd, San Jose, CA 95110, USA'
    oak = 'Oakland International Airport (OAK), 1 Sally Ride Way, Oakland, CA 94621, USA'
    missin = '{Missing}'

    if bart is None:
        bart = {}

    if commute is None:
        commute = {}

    item = {
        bart.get('place',{}).get('name','No Bart') : {
            'walk' : {
                'duration' : bart.get('commute',{}).get('duration',{}).get('text',missin),
                'distance' : bart.get('commute',{}).get('distance',{}).get('text',missin),
            },
        },
        'M+R' : {
            'drive' : {
                'duration' : commute.get('friend',{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('friend',{}).get('distance',{}).get('text',missin),
                'traffic' : commute.get('friend',{}).get('duration_in_traffic',{}).get('text',missin),
            }
        },
        'work' : {
            'drive' : {
                'duration' : commute.get('work',{}).get('drive',{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('work',{}).get('drive',{}).get('distance',{}).get('text',missin),
                'traffic' : commute.get('work',{}).get('drive',{}).get('duration_in_traffic',{}).get('text',missin),
            },
            'transit' : {
                'duration' : commute.get('work',{}).get('transit',{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('work',{}).get('transit',{}).get('distance',{}).get('text',missin),
                'fare' : commute.get('work',{}).get('transit',{}).get('fare',{}).get('text',missin),
            },
        },
        'OAK' : {
            'drive' : {
                'duration' : commute.get('airports',{}).get('drive',{}).get(oak,{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('airports',{}).get('drive',{}).get(oak,{}).get('distance',{}).get('text',missin),
                'traffic' : commute.get('airports',{}).get('drive',{}).get(oak,{}).get('duration_in_traffic',{}).get('text',missin),
            },
            'transit' : {
                'duration' : commute.get('airports',{}).get('transit',{}).get(oak,{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('airports',{}).get('transit',{}).get(oak,{}).get('distance',{}).get('text',missin),
                'fare' : commute.get('airports',{}).get('transit',{}).get(oak,{}).get('fare',{}).get('text',missin),
            },
        },
        'SJC' : {
            'drive' : {
                'duration' : commute.get('airports',{}).get('drive',{}).get(sjc,{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('airports',{}).get('drive',{}).get(sjc,{}).get('distance',{}).get('text',missin),
                'traffic' : commute.get('airports',{}).get('drive',{}).get(sjc,{}).get('duration_in_traffic',{}).get('text',missin),
            },
            'transit' : {
                'duration' : commute.get('airports',{}).get('transit',{}).get(sjc,{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('airports',{}).get('transit',{}).get(sjc,{}).get('distance',{}).get('text',missin),
                'fare' : commute.get('airports',{}).get('transit',{}).get(sjc,{}).get('fare',{}).get('text',missin),
            },
        },
        'SFO' : {
            'drive' : {
                'duration' : commute.get('airports',{}).get('drive').get(sfo).get('duration').get('text',missin),
                'distance' : commute.get('airports',{}).get('drive').get(sfo).get('distance').get('text',missin),
                'traffic' : commute.get('airports',{}).get('drive').get(sfo).get('duration_in_traffic').get('text',missin),
            },
            'transit' : {
                'duration' : commute.get('airports',{}).get('transit',{}).get(sfo,{}).get('duration',{}).get('text',missin),
                'distance' : commute.get('airports',{}).get('transit',{}).get(sfo,{}).get('distance',{}).get('text',missin),
                'fare' : commute.get('airports',{}).get('transit',{}).get(sfo,{}).get('fare',{}).get('text',missin),
            },
        },
    }
    return item

def flatten_places_stats(stats_dict, prepend_key='', total=None):
    flat = {}

    if prepend_key != '':
        prepend_key += '.'

    for k, v in stats_dict.items():
        if isinstance(v, dict):
            if 'average' in v.keys():
                if 'distance' in k:
                    flat[prepend_key + k] = convert_miles(v)
                else:
                    flat[prepend_key + k] = v
                flat[prepend_key + k]['total'] = total
            elif 'total' in v.keys():
                flat.update(flatten_places_stats(v, prepend_key + k, v['total']))
            else:
                flat.update(flatten_places_stats(v, prepend_key + k, total))
    return flat

def convert_miles(d):

    if isinstance(d, dict):
        return {k : convert_miles(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [convert_miles(i) for i in d]
    else:
        return float(d) * 0.000621371192

def merge (list1, list2):
    l = []

    length = max(len(list1),len(list2))

    for i in range(0,length):
        if i < len(list1) and i < len(list2):
            l.append(list1[i] if list1[i] else list2[i])
        elif i < len(list1):
            l.append(list1[i])
        elif i < len(list2):
            l.append(list2[i])
    return l

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


if __name__ == '__main__':
    #load_oakland_crime()
    #candidates = load_all_candidates()
    #sort()
    #menu()
    wrapper(curses_init)
    #other_curses_menu()