import json
import boto3
import os
import pandas as pd 

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    source_bucket = os.getenv('source_bucket').strip()
    destination_bucket = os.getenv('destination_bucket').strip()
    columns_to_remove = [col.strip().strip("'") for col in os.getenv('COLUMNS_TO_REMOVE', '').split(',')]
    
    print(f"Columns to Remove: {columns_to_remove}")
    
    file_key = event['Records'][0]['s3']['object']['key']
    
    json_object = s3_client.get_object(Bucket=source_bucket, Key=file_key)
    json_content = json.loads(json_object['Body'].read().decode('utf-8'))
    
    data = preprocess_json(json_content,columns_to_remove)
    
    new_file_key = file_key.replace('.json','_clean.json')
    
    s3_client.put_object(
        Bucket=destination_bucket,
        Key = new_file_key,
        Body = json.dumps(data)
    ) 
    
    return {
        'statusCode': 200
    }

def preprocess_json(data,columns_to_remove):
   
    df = pd.DataFrame(data)

    df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
    
    print(f"DataFrame after column removal: {df.head()}")
    
    df.dropna(inplace=True)

    result = df.to_dict(orient='records')

    return result
