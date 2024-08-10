import json
import boto3
import os
import pandas as pd 
import awswrangler as wr

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    source_bucket = os.getenv('source_bucket').strip()
    destination_bucket = os.getenv('destination_bucket').strip()
    columns_to_remove = [col.strip().strip("'") for col in os.getenv('COLUMNS_TO_REMOVE', '').split(',')]
    
    print(f"Columns to Remove: {columns_to_remove}")
    
    file_key = event['Records'][0]['s3']['object']['key']
    
    source_path = f"s3://{source_bucket}/{file_key}"
    
    json_content = wr.s3.read_json(path = source_path)
    
    data = preprocess_json(json_content,columns_to_remove)
    
    new_file_key = file_key.replace('.json','_clean.json')
    destination_path = f"s3://{destination_bucket}/{new_file_key}"
    
    wr.s3.to_json(
        df = data,
        path = destination_path,
        orient='records',
        lines=False
    ) 
    
    return {
        'statusCode': 200,
    }

def preprocess_json(data,columns_to_remove):
   
    df = pd.DataFrame(data)

    df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
    
    print(f"DataFrame after column removal: {df.head()}")
    
    df.dropna(inplace=True)

    return df