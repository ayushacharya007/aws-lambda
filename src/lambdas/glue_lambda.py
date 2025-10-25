import boto3
import awswrangler as wr
import pandas as pd
import os

session = boto3.Session()
glue_client = session.client('glue')
athena_client = session.client('athena')

def get_databases():
    """Function to get all Glue databases.
    
    Returns:
        list: A list of database names.
    """
    try:
        databases = []
        paginator = glue_client.get_paginator('get_databases')
        print("Database Paginator created...")
        response_iterator = paginator.paginate()
        for page in response_iterator:
            for db in page.get('DatabaseList', []):
                if db['Name'].endswith('_staging'):
                    continue
                print(f"Found database: {db['Name']}")
                databases.append(db['Name'])
        print(f"Total databases found: {len(databases)}")
        return databases
    except Exception as e:
        print(f"Error retrieving databases: {e}")
        import traceback
        print(traceback.print_exc())
        return []
    

def get_tables(database_name):
    """Function to get all tables in a Glue database.

    Args:
        database_name (str): The name of the Glue database. 

    Returns:
        list: A list of table names in the specified Glue database.
    """
    try:
        tables = []
        paginator = glue_client.get_paginator('get_tables')
        print(f"Table Paginator created for database: {database_name}...")
        response_iterator = paginator.paginate(DatabaseName=database_name)
        for page in response_iterator:
            for table in page.get('TableList', []):
                if table['Name'].startswith('_'):
                    continue
                print(f"Found table: {table['Name']} in database: {database_name}")
                tables.append(table['Name'])
            print(f"Total tables found in database {database_name}: {len(tables)}")
        return tables
    except Exception as e:
        print(f"Error retrieving tables for database {database_name}: {e}")
        import traceback
        print(traceback.print_exc())
        return []


def get_row_count(all_tables, batch_size=20):
    """Function to execute Athena queries in batches and get row counts for all tables.

    Args:
        all_tables (dict): A dictionary with database names as keys and list of table names as values.
        batch_size (int): Number of queries to batch together (default: 20).

    Returns:
        list: A list of query execution IDs for all batched queries, or empty list if no queries were executed.
    """
    try:
        # Write SQL query to get the count of rows in each table of the databases
        print("Preparing to execute row count queries...")
        sql_queries = []
        for db_name, tables in all_tables.items():
            for table_name in tables:
                sql_queries.append(f"SELECT '{db_name}' AS database_name, '{table_name}' AS table_name, COUNT(*) AS row_count FROM {db_name}.{table_name}")
        
        if not sql_queries:
            print("No tables found to query.")
            return []
        
        # Process queries in batches
        query_execution_ids = []
        total_batches = (len(sql_queries) + batch_size - 1) // batch_size # return ceiling of division
        print(f"Total tables to process: {len(sql_queries)}")
        print(f"Batch size: {batch_size}")
        print(f"Total batches: {total_batches}")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size # starting index for the batch
            end_idx = min((batch_num + 1) * batch_size, len(sql_queries)) # ending index for the batch
            batch_queries = sql_queries[start_idx:end_idx]
            
            union_query = " UNION ALL ".join(batch_queries)
            print(f"\nExecuting batch {batch_num + 1}/{total_batches} (queries {start_idx + 1} to {end_idx}):")
            print(f"Query preview: {union_query}")
            
            try:
                response = athena_client.start_query_execution(
                    QueryString=union_query,
                    ResultConfiguration={
                        'OutputLocation': f's3://glue-lambda-bucket-{os.getenv("BUCKET_NAME")}/athena-results/'  # S3 bucket for Athena results
                    },
                    WorkGroup='primary',  # Replace with your Athena workgroup
                    ResultReuseConfiguration={
                        'ResultReuseByAgeConfiguration': {
                            'Enabled': True,
                            'MaxAgeInMinutes': 1440  # 1 day
                        }
                    }
                )
                query_execution_id = response['QueryExecutionId']
                query_execution_ids.append(query_execution_id)
                print(f"Batch {batch_num + 1} query started with Execution ID: {query_execution_id}")
            except Exception as batch_error:
                print(f"Error executing batch {batch_num + 1}: {batch_error}")
                import traceback
                print(traceback.print_exc())
                continue
        
        print(f"\nAll {len(query_execution_ids)} batches submitted successfully.")
        return query_execution_ids
        
    except Exception as e:
        print(f"Error in get_row_count: {e}")
        import traceback
        print(traceback.print_exc())
        return []

def get_csv_result(query_execution_id):
    """Function to retrieve the CSV result from S3 for a given Athena query execution ID.

    Args:
        query_execution_id (str): The Athena query execution ID.
    Returns:
        str: The CSV content as a string.
    """
    try:
        while True:
            print(f"Checking status for query execution ID: {query_execution_id}...")
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            print(f"Current state: {state}")
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            print(f"Query {query_execution_id} is still running. Current state: {state}")
            import time
            time.sleep(30)  # Wait before checking again
            
        if state == 'SUCCEEDED':
            s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            print(f"S3 Path for results: {s3_path}")
            s3_response = wr.s3.read_csv(path=s3_path)
            return s3_response
        else:
            print(f"Query {query_execution_id} did not succeed. Final state: {state}")
            return None
    except Exception as e:
        print(f"Error retrieving CSV result for query {query_execution_id}: {e}")
        import traceback
        print(traceback.print_exc())
        return None

def handler(event, context):
    
    try:
        print("Code starting...")
        
        print("Client created, listing databases and tables...")
        databases = get_databases()
        print(f"Successfully listed databases.")
        
        print("Listing tables for each database...")
        all_tables = {}
        for db_name in databases:
            tables = get_tables(db_name)
            all_tables[db_name] = tables
            
        # Get row counts for all tables using Athena (in batches)
        query_execution_ids = get_row_count(all_tables, batch_size=20)
        
        if not query_execution_ids:
            print("No query execution IDs returned. Skipping result retrieval.")
            return {
                'statusCode': 400,
                'body': "No tables found to query.",
            }
        
        print(f"Processing {len(query_execution_ids)} batch queries...")
        all_results = []
        
        # Process results from all batches
        for idx, query_execution_id in enumerate(query_execution_ids):
            print(f"\nProcessing batch result {idx + 1}/{len(query_execution_ids)} (Execution ID: {query_execution_id})...")
            csv_df = get_csv_result(query_execution_id)
            if csv_df is not None:
                print(f"Batch {idx + 1} result retrieved successfully. Rows: {len(csv_df)}")
                all_results.append(csv_df)
            else:
                print(f"Batch {idx + 1} result is None, skipping...")
        
        if all_results:
            print(f"\nCombining results from all {len(all_results)} batches...")    
            combined_df = pd.concat(all_results, ignore_index=True)
            print(f"Combined CSV Result ({len(combined_df)} rows total):")
            print(combined_df.head())
            print("Converting combined CSV result to Parquet and storing in Glue table...")
            parquet_path = f"s3://glue-lambda-bucket-{os.getenv('BUCKET_NAME')}/data-schema/combined-results.parquet"
            wr.s3.to_parquet(
                df=combined_df,
                path=parquet_path,
                index=False,
                dataset=True,
                mode='overwrite',
                database="data-monitoring-database",
                table="data_monitoring_table"
            )
            print(f"Parquet file stored at: {parquet_path}")
            return {
                'statusCode': 200,
                'body': "Successfully listed databases and tables.",
            }
        else:
            print("No CSV results retrieved from any batch.")
            return {
                'statusCode': 500,
                'body': "Failed to retrieve CSV results from Athena batches.",
            }
        
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        print(traceback.print_exc())
        return {
            'statusCode': 500,
            'body': f"Error occurred: {e}",
        }