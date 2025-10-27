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


def get_row_count(all_tables):
    """Function to execute Athena queries in batches and get row counts for all tables.

    Args:
        all_tables (dict): A dictionary with database names as keys and list of table names as values.

    """
    try:
        # Write SQL query to get the count of rows in each table of the databases
        print("Preparing to execute row count queries...")
        sql_queries = []
        combined_df = pd.DataFrame()
        for db_name, tables in all_tables.items():
            for table_name in tables:
                sql_queries.append(f"SELECT '{db_name}' AS database_name, '{table_name}' AS table_name, COUNT(*) AS row_count FROM {db_name}.{table_name}")
            final_query = " UNION ALL ".join(sql_queries)
            try:
                df = wr.athena.read_sql_query(
                    sql=final_query,
                    database=db_name,
                    workgroup='primary'
                )
                df['last_updated'] = pd.Timestamp.now()
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                print(f"Error executing query for database {db_name}: {e}")
                continue
        wr.s3.to_parquet(
            df=combined_df,
            path=f"s3://{os.getenv('BUCKET_NAME')}/data-schema/data-monitoring.parquet",
            index=False,
            dataset=True,
                mode='append',
                database=os.getenv("GLUE_DATABASE_NAME"),
                table="data_monitoring_table"
            )
        print("Row count queries executed and results stored in Glue table.")
    except Exception as e:
        if not combined_df.empty:
            print("An error occurred, but partial results are available.")
            wr.s3.to_parquet(
                df=combined_df,
                path=f"s3://{os.getenv('BUCKET_NAME')}/data-schema/data-monitoring.parquet",
                index=False,
                dataset=True,
                mode='append',
                database=os.getenv("GLUE_DATABASE_NAME"),
                table="data_monitoring_table"
            )
        print(f"Error executing row count queries: {e}")
        import traceback
        print(traceback.print_exc())


def handler(event, context):
    
    try:
        print("Code starting...")
        
        print("Client created, listing databases and tables...")
        databases = get_databases()
        print("Successfully listed databases.")
        
        print("Listing tables for each database...")
        all_tables = {}
        for db_name in databases:
            tables = get_tables(db_name)
            all_tables[db_name] = tables

        if len(all_tables) > 0:
            get_row_count(all_tables)
           
        return {
            'statusCode': 200,
            'body': "Successfully listed databases and tables.",
        }
        
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        print(traceback.print_exc())
        return {
            'statusCode': 500,
            'body': f"Error occurred: {e}",
        }