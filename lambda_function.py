import json
import boto3
import requests
from datetime import datetime

# Initialize Kinesis client
firehose = boto3.client('firehose')

def lambda_handler(event, context):
    # 1. Fetch data from Public API (CoinGecko)
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_vol=true"
    response = requests.get(url)
    data = response.json()
    
    # 2. Add metadata (timestamp)
    data['timestamp'] = datetime.now().isoformat()
    
    # 3. Prepare for Kinesis (must be bytes + newline for S3 readability)
    payload = json.dumps(data) + "\n"
    
    # 4. Push to Firehose
    firehose.put_record(
        DeliveryStreamName='PUT-S3-mA5la',
        Record={'Data': payload}
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data sent to Kinesis!')
    }
