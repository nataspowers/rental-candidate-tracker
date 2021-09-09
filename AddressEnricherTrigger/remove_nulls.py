import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Candidates')
scan_kwargs = {
    'FilterExpression': Key('status').eq('active')
}

done = False
start_key = None
items = []
while not done:
    if start_key:
        scan_kwargs['ExclusiveStartKey'] = start_key
    response = table.scan(**scan_kwargs)
    items = items + response.get('Items', [])
    start_key = response.get('LastEvaluatedKey', None)
    done = start_key is None


for i in items:
    for k, v in i.items():
        if v is None:
            update_expr = 'remove {}'.format(k)
            response = table.update_item(
                            Key={'Address': i['Address']},
                                UpdateExpression=update_expr,
                            )
            print('{} from {}'.format(update_expr, i['Address']))
        elif isinstance(v, dict):
            for k1, v1 in v.items():
                if v1 is None:
                    update_expr = 'remove {}.{}'.format(k, k1)
                    response = table.update_item(
                                Key={'Address': i['Address']},
                                UpdateExpression=update_expr,
                            )
                    print('{} from {}'.format(update_expr, i['Address']))

