import awswrangler as wr
import json
import os

def handler(event, context):
    
    try:
        print(f"SNS Lambda started. Processing {len(event['Records'])} SNS records")

        print(f"Event: {event}")
        print(f"Context: {context}")

        print("Processing SNS records...")
        for record in event['Records']:
            print("Parsing SNS message body...")
            print(f"SNS Record: {record}")
            # Parse the SNS message body which contains the S3 notification
            message_body = json.loads(record['Sns']['Message'])
            
            print(f"Parsed message body: {message_body}")
            
            # Extract S3 event records from the parsed message
            for s3_record in message_body['Records']:
                
                print(f"S3 Record: {s3_record}")
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                
                s3_path = f"s3://{bucket}/{key}"
                print(f"Processing file: {s3_path}")
                
                # Read the CSV file from S3
                print("Reading CSV from S3...")
                df = wr.s3.read_csv(
                    s3_path,
                    usecols=['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'rating']
                )
                print(f"CSV read successfully. Data shape: {df.shape}")
            
                
                print("Cleaning data...")
                df.drop_duplicates(inplace=True)
                df.dropna(inplace=True)  
                print(f"Data cleaned. Final shape: {df.shape}")
                print(f"Successfully processed files: {s3_path}")

                output_path = f"s3://{bucket}/Transformed/{os.path.basename(key.split('.')[0])}.parquet"
                print(f"Writing to S3: {output_path}")

                wr.s3.to_parquet(
                    df,
                    path=output_path,
                )
                
                print(f"Successfully wrote file to: {output_path}")
                
                
        print("All records processed successfully")
        return {
            'statusCode': 200,
            'body': f'Successfully transformed and saved {len(event["Records"])} file(s)'
        }
            
    except Exception as e:
        error_msg = f"Error processing file: {e}"
        print(f"Exception occurred: {error_msg}")
        print(f"Exception type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': error_msg
        }


