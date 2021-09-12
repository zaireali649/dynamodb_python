import requests
import time

def call_ISS_API():
    print("Calling ISS")
    time.sleep(1)
    r = requests.get('http://api.open-notify.org/iss-now.json')
    json = r.json()
    json['latitude'] = json['iss_position']['latitude']
    json['longitude'] = json['iss_position']['longitude']
    del json['iss_position']
    del json['message']
    return json    
    
api_calls = {}

for i in range(5):    
    response = call_ISS_API()
    api_calls[response['timestamp']] = response    
    
#%%
import boto3

# replace the keys below

client = boto3.client(
    'dynamodb',
    aws_access_key_id='*****',
    aws_secret_access_key='*****',
    )

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='*****',
    aws_secret_access_key='*****',
    )
    
ddb_exceptions = client.exceptions

#%%

try:
    table = client.create_table(
        TableName='ISS_locations',
        KeySchema=[
            {
                'AttributeName': 'timestamp',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'N'
            }
    
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    print("Creating table")
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName='ISS_locations')
    print("Table created")
    
except ddb_exceptions.ResourceInUseException:
    print("Table exists")
    
#%%

print("Putting items")
for response in api_calls:
    dynamodb.Table('ISS_locations').put_item(
        Item=api_calls[response]
        )
    
#%%

print("Scanning table")
response = dynamodb.Table('ISS_locations').scan()

for i in response['Items']:
    print(i)
#%%

print("Query table")
from boto3.dynamodb.conditions import Key

k = api_calls[list(api_calls)[0]]['timestamp']

response = dynamodb.Table('ISS_locations').query(
    KeyConditionExpression=Key('timestamp').eq(k)
)

for i in response['Items']:
    print(i)    

#%%

print("Deleting Table")
client.delete_table(TableName='ISS_locations')
waiter = client.get_waiter('table_not_exists')
waiter.wait(TableName='ISS_locations')
print("Table deleted")
