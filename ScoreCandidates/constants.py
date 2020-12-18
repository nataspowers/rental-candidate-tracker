weights_test = {
    'all' : {
        'walk' : 1,
    }
}

weights = {
    'all': {
        'area' : 1,
        'ppsf' : 1,
        'rent' : 1,
        'deposit' : 1,
        'year_built' : 1,
        'pets': 1,
        'property_type': 1,
        'ac': 1,
        'fitness': 1,
        'days_on_market': 1,
        'bedrooms': 1,
        'bathrooms': 1,
        'attributes': 1,
        'crime': 1,
        'walk':1,
        'bart' : 1,
        'bart.duration':1,
        'bart.distance':1,
        'bart.rating':1,
        'visual':1,
        'bath':1,
        'kitchen' : 1,
        'livingroom' : 1,
        'restaurant' : 1,
        'restaurant.stats.price.$': 1,
        'restaurant.stats.price.$$': 1,
        'restaurant.stats.price.$$$': 1,
        'restaurant.stats.price.$$$$': 1,
        'restaurant.stats.rating.1': 1,
        'restaurant.stats.rating.2': 1,
        'restaurant.stats.rating.3': 1,
        'restaurant.stats.rating.4': 1,
        'restaurant.stats.distance.0':1,
        'restaurant.stats.distance.1':1,
        'restaurant.stats.distance.2':1,
        'restaurant.stats.distance.3':1,
        'restaurant.stats.distance.4':1,
        'restaurant.stats.distance.5':1,
        'restaurant.categories':1,
        'restaurant.distance':1,
        'restaurant.rating': 1,
        'coffee' : 1,
        'coffee.stats.price.$': 1,
        'coffee.stats.price.$$': 1,
        'coffee.stats.price.$$$': 1,
        'coffee.stats.price.$$$$': 1,
        'coffee.stats.rating.1': 1,
        'coffee.stats.rating.2': 1,
        'coffee.stats.rating.3': 1,
        'coffee.stats.rating.4': 1,
        'coffee.stats.distance.0':1,
        'coffee.stats.distance.1':1,
        'coffee.stats.distance.2':1,
        'coffee.stats.distance.3':1,
        'coffee.stats.distance.4':1,
        'coffee.stats.distance.5':1,
        'coffee.distance':1,
        'coffee.rating': 1,
        'convenience_store':1,
        'work':1,
        'work.drive':1,
        'work.transit':1,
        'friend':1,
        'airports':1,
        'airports.drive':1,
        'airports.transit':1,
        'airports.OAK':1,
        'airports.OAK.drive':1,
        'airports.OAK.transit':1,
        'airports.SFO':1,
        'airports.SFO.drive':1,
        'airports.SFO.transit':1,
        'airports.SJC':1,
        'airports.SJC.drive':1,
        'airports.SJC.transit':1,
    },
    'joe' : {
        'ppsf' : 10,
        'rent' : 5,
        'area' : 5,
        'deposit' : 1,
        'year_built' : 4,
        'pets': 2,
        'property_type': 5,
        'ac': 5,
        'fitness': 2,
        'bedrooms': 5,
        'bathrooms': 5,
        'attributes': 1,
        'crime': 8,
        'bart.duration' : 8,
        'kitchen' : 8,
        'livingroom' : 5,
        'restaurant.categories':3,
        'restaurant.rating': 1,
        'coffee.rating': 2,
        'coffee.distance.0' : 5,
        'coffee.distance.1' : 5,
        'airports.transit': 5,
        'work.drive':6,
        'work.transit':10,
        'friend':10,
        'walk':7,
        'airports.OAK.transit':3,
    },
    'jen':{
        'area': 5,
        'airports.OAK': 5,
        'airports.SJC': 3,
        'friend':3,
        'ac':10,
        'bart.duration':7,
        'restaurant.categories': 7,
        'restaurant.rating':7,
        'attributes':5,
        'walk':5,
        'fitness':7,
        'bath':7,
        'bathrooms':5,
        'bedrooms':5,
        'pets':10,
        'crime':8,
        'convenience_store':1,
    }
}

property_type = {
    'townhouse': 7,
    'apartment': 2,
    'single family home': 10,
    'single family home single family house': 10,
    'condo': 8,
    'multi family': 4,
}

restaurant_cat_types = {
    'all' : {
        'newamerican':1,'ramen':1,'burgers':1,'breakfast_brunch':1,'mexican':1,
        'creperies':1,'gastropubs':1,'whiskeybars':1,'pizza':1,'comfortfood':1,'italian':1,
        'cafes':1,'salad':1,'pubs':1,'french':1,'spanish':1,'wine_bars':1,'sandwiches':1,
        'japanese':1,'chickenshop':1,'asianfusion':1,'sportsbars':1,'noodles':1,'sushi':1,
        'meats':1,'butcher':1,'seafood':1,'pakistani':1,'chinese':1,'diners':1,'bakeries':1,
        'panasian':1,'breweries':1,'mediterranean':1,'delis':1,'ethiopian':1,'tacos':1,
        'mideastern':1,'bagels':1,'gelato':1,'soup':1,'bbq':1,'latin':1,'cajun':1,'brazilian':1,
        'tex-mex':1,'southern':1,'poke':1,'falafel':1,'wraps':1,'persian':1,'cheesesteaks':1,
        'dimsum':1,'gourmet':1,'british':1,'steak':1,'teppanyaki':1,'tapas':1,
        'german':1, 'hotdog':1,'hotdogs':1,'izakaya':1,'fishnchips':1,'salvadoran':1,
        'soulfood':1,'caribbean':1,'halal':1,'streetvendors':1,'foodtrucks':1,
        'popuprestaurants':1,'indpak':1,'gluten_free':1,'popupshops':1,'srilankan':1,
        'kitchenincubators':1,'acaibowls':1,'beer_and_wine':1,'arabian':1,'tradamerican':1,
        'lebanese':1,'tapasmallplates':1,'donuts':1,'vegan':1,'deptstores':1,'afghani':1,
        'farmersmarket':1,'foodstands':1,'grocery':1,'musicvenues':1,'juicebars':1,'kebab':1,
        'cookingclasses':1,'desserts':1,'festivals':1,'waffles':1,'cocktailbars':1,
        'bike_repair_maintenance':1,'peruvian':1,'filipino':1,'bubbletea':1,'beerbar':1,'thai':1,
        'icecream':1,'beergardens':1,'danceclubs':1,'cosmetics':1,'tikibars':1,'jazzandblues':1,
        'greek':1,'buffets':1,'korean':1,'cantonese':1,'bars':1,'dinnertheater':1,'coffee':1,
        'catering':1,'chicken_wings':1,'venues':1,'herbsandspices':1,'burmese':1,'taiwanese':1,
        'african':1,'convenience':1,'tea':1,'coffeeroasteries':1,'hawaiian':1,'vegetarian':1,
        'vietnamese':1
    },
    'joe' : {
        'newamerican':7,'burgers':6,'breakfast_brunch':8,'mexican':6,
        'creperies':9,'gastropubs':5,'whiskeybars':8,'pizza':2,'comfortfood':4,
        'italian':6,'cafes':6,'salad':3,'pubs':3,'french':7,'spanish':6,
        'wine_bars':5,'sandwiches':6,'japanese':9,'chickenshop':2,'asianfusion':1,
        'sportsbars':3,'sushi':10,'meats':6,'butcher':8,'seafood':8,'german':7,
        'pakistani':2,'chinese':1,'diners':5,'bakeries':3,'panasian':1,'breweries':4,
        'mediterranean':7,'delis':5,'ethiopian':5,'tacos':2,'mideastern':5,'bagels':3,
        'gelato':2,'soup':5,'bbq':8,'latin':5,'cajun':5,'brazilian':6,'tex-mex':5,
        'southern':4,'poke':9,'wraps':5,'persian':4,'cheesesteaks':8,'dinnertheater':10,
        'dimsum':8,'gourmet':9,'british':4,'steak':10,'teppanyaki':8,'tapas':4,'kebab':6,
        'desserts':3,'cocktailbars':4,'greek':2,'hawaiian':5
    },
    'jen' : {
        'newamerican':4,'ramen':7,'burgers':2,'breakfast_brunch':8,'mexican':3,
        'creperies':9,'gastropubs':5,'whiskeybars':8,'comfortfood':4,
        'italian':6,'cafes':6,'salad':3,'pubs':3,'french':7,'spanish':6,
        'wine_bars':5,'sandwiches':6,'japanese':9,'asianfusion':5,
        'sportsbars':1,'noodles':8,'sushi':7,'seafood':8,
        'pakistani':4,'chinese':3,'diners':5,'bakeries':2,'panasian':1,'breweries':4,
        'mediterranean':7,'delis':5,'ethiopian':5,'tacos':2,'mideastern':5,'bagels':3,
        'gelato':2,'soup':5,'bbq':8,'latin':5,'cajun':5,'brazilian':6,'tex-mex':5,
        'southern':4,'poke':9,'falafel':2,'wraps':5,'persian':4,'cheesesteaks':8,
        'dimsum':8,'gourmet':9,'british':4,'steak':2,'teppanyaki':8,'tapas':6,
        'tapasmallplates':6,'vegan':8,'kebab':2,'waffles':5,'desserts':5,'greek':3,
        'korean':4,'dinnertheater':5,'farmersmarket':7,'indpak':5,'vegetarian':2,
        'vietnamese':4
    }
}

observation_types = ['visual','kitchen','bath','livingroom']

commute_types = ['work','friend','airports','work.drive','work.transit','airports.OAK',
                 'airports.SJC','airports.SFO','airports.drive','airports.transit',
                 'airports.OAK.drive','airports.OAK.transit','airports.SJC.drive',
                 'airports.SJC.transit','airports.SFO.drive','airports.SFO.transit']

attributes_scores = {
    'all' :{
        "It's walkable to grocery stores": 1,
        "It's dog friendly": 1,
        "It's walkable to restaurants": 1,
        "Neighbors are friendly": 1,
        "People would walk alone at night": 1,
        "Streets are well-lit": 1,
        "Parking is easy": 1,
        "There are sidewalks": 1,
        "It's quiet": 1,
        "Yards are well-kept": 1,
        "There's wildlife" : 1,
        "There's holiday spirit": 1,
        "They plan to stay for at least 5 years": 1,
        "Car is needed": 1,
        "There are community events": 1,
        "Kids play outside": 1,
    },
    'joe' : {
        "It's walkable to grocery stores": 4,
        "It's walkable to restaurants": 8,
        "People would walk alone at night": 5,
        "Parking is easy": 7,
        "There are sidewalks": 5,
        "It's quiet": 7,
        "Car is needed": -5,
        "There are community events": -1,
        "Kids play outside": -1,
    },
    'jen': {
        "It's dog friendly": 8,
        "It's walkable to restaurants": 4,
        "People would walk alone at night": 8,
        "Streets are well-lit": 4,
        "Parking is easy": 3,
        "There are sidewalks": 2,
        "It's quiet": 7,
        "There's wildlife" : 8,
        "Car is needed": -5,
    }
}

all_attributes = list(set(list(attributes_scores['all'])))

# just in case we weighted an item that wasn't being stat'ed
all_weights =  set(list(weights['all']))
types = list(all_weights)
print('all types = {}'.format(types))

lower_better_types = ['rent','year_built','deposit', 'ppsf','days_on_market','crime','bart','bart.distance','bart.duration']
lower_better_types += commute_types

boolean_types = ['pets','ac','fitness']