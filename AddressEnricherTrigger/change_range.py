import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Candidates')
scan_kwargs = {
    'FilterExpression': Attr('observations.joe').exists()
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


print('Have {} to update'.format(len(items)))
for i in items:

    obv = dict()
    for key, val in i.get('observations',{}).get('joe',{}).items():
        if isinstance(val,Decimal):
            if 1 <= int(val) <= 5:
                val = Decimal(int(val) + 4)
        elif val.isnumeric():
            if 1 <= int(val) <= 5:
                val = str(int(val) + 4)
        obv[key] = val

    if obv:
        print('updating {}, obv {}'.format(i['Address'],obv))
        try:
            table.update_item(
                Key={
                        'Address': i['Address'],
                    },
                UpdateExpression="set observations.joe = :a",
                ExpressionAttributeValues={
                        ':a': obv
                    }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise