import boto3
import os
import logging
import json
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Athena client outside handler for reuse
try:
    client = boto3.client('athena')
except Exception as e:
    logger.error(f"Failed to initialize Athena client: {e}")
    raise

BUCKET_NAME = os.getenv('BUCKET_NAME', 's3-glue-test-180294223557-ap-southeast-2')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'data_monitoring_db')
BATCH_SIZE = 50 # MAX LIMIT

def get_all_query_ids(workgroup='primary'):
    """
    Retrieves all query execution IDs from the specified Athena workgroup using pagination.
    """
    query_ids = []
    try:
        logger.info(f"Starting to fetch query IDs from workgroup: {workgroup}")
        logger.info(f"Configuration - Bucket: {BUCKET_NAME}, Database: {DATABASE_NAME}")

        response = client.list_query_executions(WorkGroup=workgroup)
        
        next_token = response.get('NextToken')
        query_ids.extend(response['QueryExecutionIds'])

        while next_token and len(query_ids) <= 100:
            response = client.list_query_executions(WorkGroup=workgroup, NextToken=next_token)
            next_token = response.get('NextToken')
            query_ids.extend(response['QueryExecutionIds'])

        logger.info(f"Successfully retrieved total {len(query_ids)} query IDs")
        
        # save the list in a file
        with open('query_ids.txt', 'w') as f:
            for query_id in query_ids:
                f.write(f"{query_id}\n")
        return query_ids
    except ClientError as e:
        logger.error(f"AWS ClientError in get_all_query_ids: {e}")
        raise
    except BotoCoreError as e:
        logger.error(f"AWS BotoCoreError in get_all_query_ids: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_all_query_ids: {e}")
        raise
    
    
def get_query_details(query_id: list):
    """
    Retrieves details of a specific query execution using its ID.
    """
    try:
        logger.info(f"Starting to fetch query details for {len(query_id)} query IDs")
        response = client.batch_get_query_execution(QueryExecutionIds=query_id)
        
        # Create a list to store the extracted details
        extracted_list = []
        
        # Extract the list of executions from the response
        executions = response.get("QueryExecutions", [])
        if not executions and "QueryExecutionId" in response:
             executions = [response]

        # Loop through the list of executions and extract the details
        for query in executions:
            extracted = {
                "ID": query.get("QueryExecutionId"),
                "Query": query.get("Query"),
                "State": query.get("Status", {}).get("State"),
                "Date": str(query.get("Status", {}).get("SubmissionDateTime")),
                "RunTime": f"{query.get('Statistics', {}).get('TotalExecutionTimeInMillis', 0) / 1000} sec",
                "DataScanned": f"{query.get('Statistics', {}).get('DataScannedInBytes', 0)} bytes",
                "WorkGroup": query.get("WorkGroup"),
                "QueryType": query.get("StatementType"),
                "QueryOutputLocation": query.get("ResultConfiguration", {}).get("OutputLocation"),
            }
            extracted_list.append(extracted)
            
        logger.info(f"Successfully retrieved {len(extracted_list)} queries details")
        return extracted_list

    except ClientError as e:
        logger.error(f"AWS ClientError in get_query_details: {e}")
        raise
    except BotoCoreError as e:
        logger.error(f"AWS BotoCoreError in get_query_details: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_query_details: {e}")
        raise
    
def handler(event, context):
    try:
        ids = get_all_query_ids()
        all_query_details = []
        
        for i in range(0, len(ids), BATCH_SIZE):
            batch = ids[i:i + BATCH_SIZE]
            batch_details = get_query_details(batch)
            all_query_details.extend(batch_details)
            
        # Process all details into a DataFrame
        if all_query_details:
            df = pd.DataFrame(all_query_details)
            df["last_updated"] = pd.Timestamp.now()
            
            logger.info("Successfully retrieved all query details")
            logger.info(df.head())
            
            # Save to JSON
            with open("query_details.json", "w") as f:
                json.dump(all_query_details, f, indent=4)
        else:
            logger.info("No query details found.")

    except Exception as e:
        logger.error(f"Execution failed: {e}")