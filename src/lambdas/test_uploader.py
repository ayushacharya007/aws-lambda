import boto3
from botocore.exceptions import ClientError
import os

session = boto3.Session()
s3_client = session.client('s3')
# Use relative path - files are now packaged with the Lambda
PATH = "../resources"

def handler(event, context):
    try:
        bucket_name = os.environ.get('BUCKET_NAME')
        if not bucket_name:
            return {
                'statusCode': 400,
                'body': 'BUCKET_NAME environment variable not set'
            }
            
        uploaded_files = []
        for root, dirs, files in os.walk(PATH):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, PATH)
                print(f"Uploading: {relative_path}")
                
                s3_client.upload_file(
                    Filename=local_path,
                    Bucket=bucket_name,
                    Key=f"uploaded-data/{relative_path}"
                )
                uploaded_files.append(relative_path)
                
        return {
            'statusCode': 200,
            'body': f'Successfully uploaded {len(uploaded_files)} files: {uploaded_files}'
        }
        
    except ClientError as e:
        error_msg = f"Error uploading files: {e}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        }
        