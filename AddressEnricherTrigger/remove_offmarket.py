import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Candidates')
scan_kwargs = {
    'FilterExpression': Key('status').eq('off-market')
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


print('Have {} to delete'.format(len(items)))
for i in items:
    if i['status'] == 'off-market':
        try:
            response = table.delete_item(
                Key={
                    'Address': i['Address']
                },
                ConditionExpression="#s1 = :val",
                ExpressionAttributeNames = {
                  '#s1':'status'
                },
                ExpressionAttributeValues={
                    ":val": 'off-market'
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                raise
        else:
            print('Successfully deleted {}'.format(i['Address']))

