import awswrangler as wr
import os

def handler(event, context):
    """
    Handler for EventBridge S3 events.
    Processes S3 objects by reading CSV, cleaning data, and saving as Parquet.
    
    Event structure (from EventBridge):
    {
        "detail-type": "Object Created",
        "source": "aws.s3",
        "detail": {
            "bucket": {"name": "bucket-name"},
            "object": {"key": "object-key", "size": 123, ...}
        }
    }
    """
    try:
        print("Lambda started...")
        print(f"Event: {event}")

        # Extract bucket and key from EventBridge event
        detail = event.get('detail', {})
        bucket = detail.get('bucket', {}).get('name')
        key = detail.get('object', {}).get('key')
        
        if not bucket or not key:
            print("Error: Missing bucket or key in event")
            return {
                'statusCode': 400,
                'body': 'Missing bucket or key in event',
            }

        s3_path = f"s3://{bucket}/{key}"
        print(f"Processing file: {s3_path}")
        
        # Read the CSV file from S3
        print("Reading CSV from S3...")
        df = wr.s3.read_csv(
            s3_path,
            usecols=['show_id', 'type', 'title', 'director', 'cast', 'country', 'release_year', 'rating']
        )
        print(f"CSV read successfully. Data shape: {df.shape}")
    
        # Clean data
        print("Cleaning data...")
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)  
        print(f"Data cleaned. Final shape: {df.shape}")

        # Write cleaned data to Parquet
        file_name = os.path.basename(key).split('.')[0]
        DESTINATION_BUCKET_NAME = os.getenv("DESTINATION_BUCKET_NAME")
        output_path = f"s3://{DESTINATION_BUCKET_NAME}/Transformed/{file_name}.parquet"
        print(f"Writing to S3: {output_path}")

        wr.s3.to_parquet(
            df,
            path=output_path,
        )
        
        print(f"Successfully wrote file to: {output_path}")
                
        return {
            'statusCode': 200,
            'body': f'Successfully transformed and saved file: {key}',
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


