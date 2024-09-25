from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.minio_service import create_client
from plugins.minio_service import create_bucket_minio
from pymongo import MongoClient


def load_to_postgres(username: str, 
                    password: str, 
                    host: str, 
                    database: str, 
                    port: str, 
                    table_name: str) -> None:
    """
    Load data into a PostgreSQL database from a MinIO bucket.

    This function retrieves a processed Parquet file from a specified MinIO bucket, 
    loads it into a Pandas DataFrame, and inserts new records into a PostgreSQL 
    database table. If the database does not exist, it will be created. The function 
    checks for existing records in the specified table and only inserts new records 
    that are not already present.

    Parameters:
    - username (str): The username for authenticating with the PostgreSQL database.
    - password (str): The password for authenticating with the PostgreSQL database.
    - host (str): The hostname or IP address of the PostgreSQL server.
    - database (str): The name of the PostgreSQL database to create/use.
    - port (str): The port number for connecting to the PostgreSQL server.
    - table_name (str): The name of the table within the database where data will be inserted.

    Returns:
    - None: The function performs database operations and does not return a value.
    """
    import pandas as pd
    from io import BytesIO
    from datetime import datetime
    from sqlalchemy import create_engine
    from sqlalchemy.exc import ProgrammingError

    minio_bucket = 'ohitv-processed'
    client = create_client()
    create_bucket_minio(client=client, minio_bucket=minio_bucket)

    processed_ohitv_object = client.get_object(minio_bucket,"ohitv_request_processed.parquet")
    processed_ohitv_df = pd.read_parquet(BytesIO(processed_ohitv_object.read()))

    current_date = datetime.now().date()
    processed_ohitv_df['date_crawl'] = current_date
    processed_ohitv_df['quality'] = processed_ohitv_df['quality'].str.replace(" ","")

    # Create connection string for the PostgreSQL database
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/postgres"  # Connect to the default 'postgres' database
    engine = create_engine(db_connection_string)

    # Create database if not exists
    try:
        # Establish a separate connection to execute the CREATE DATABASE command
        with engine.connect() as conn:
            conn.execute("COMMIT")
            conn.execute(f"CREATE DATABASE {database};")
            print(f"Database '{database}' created.")
    except ProgrammingError as e:
        if 'database already exists' in str(e):
            print(f"Database '{database}' already exists.")
        else:
            print("Error occurred:", e)

    # Now connect to the newly created or existing database
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_connection_string)

    try:
        # Load existing titles from PostgreSQL
        existing_titles_query = f"SELECT id FROM {table_name}"
        existing_titles_df = pd.read_sql(existing_titles_query, engine)
    except Exception as e:
        print(e)
        processed_ohitv_df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)
        return ("create table and inserted already")
    
    if len(existing_titles_df) == 0:
        processed_ohitv_df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)
        return ("inserted already")

    # Identify new records by excluding existing titles
    new_records_df = processed_ohitv_df[~processed_ohitv_df['id'].isin(existing_titles_df['id'])]

    if not new_records_df.empty:
        new_records_df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Appended {len(new_records_df)} new records to the '{table_name}' table.")
        print(new_records_df[['id', 'title']].drop_duplicates('id'))
    else:
        print("No new records to append.")

def load_to_mongodb(username: str, 
                    password: str, 
                    database: str, 
                    collection: str, 
                    host: str, 
                    port: str) -> None:
    """
    Load data into a MongoDB collection from a MinIO bucket.

    This function retrieves a processed Parquet file from a specified MinIO bucket, 
    loads it into a Pandas DataFrame, and inserts new records into a MongoDB collection. 
    If the collection is empty, it inserts all records from the DataFrame. 
    Otherwise, it only inserts records that are not already present in the collection.

    Parameters:
    - username (str): The username for authenticating with the MongoDB database.
    - password (str): The password for authenticating with the MongoDB database.
    - database (str): The name of the MongoDB database to use.
    - collection (str): The name of the collection within the database where data will be inserted.
    - host (str): The hostname or IP address of the MongoDB server.
    - port (str): The port number for connecting to the MongoDB server.

    Returns:
    - None: This function does not return a value but performs database operations, 
            including inserting records into the MongoDB collection.
    """
    import pandas as pd
    from io import BytesIO
    from datetime import datetime

    minio_bucket = 'ohitv-processed'
    client = create_client()
    create_bucket_minio(client=client, minio_bucket=minio_bucket)

    processed_ohitv_object = client.get_object(minio_bucket,"ohitv_request_processed.parquet")
    processed_ohitv_df = pd.read_parquet(BytesIO(processed_ohitv_object.read()))
    processed_ohitv_df['published_date'] = processed_ohitv_df['published_date'].fillna(value='None')

    current_date = datetime.now()
    processed_ohitv_df['date_crawl'] = current_date
    processed_ohitv_df['quality'] = processed_ohitv_df['quality'].str.replace(" ","")

    connection_string = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
    client = MongoClient(connection_string)
    db = client[f"{database}"]
    collection = db[f"{collection}"]

    data_dict = processed_ohitv_df.to_dict('records')

    existing_id_cursor = collection.find({}, {"id": 1, "_id": 0})
    existing_id = [doc["id"] for doc in existing_id_cursor]

    if len(existing_id) == 0:
        collection.insert_many(data_dict)
        return ("create table and inserted already")

    # Identify new records by excluding existing titles
    new_records_df = processed_ohitv_df[~processed_ohitv_df['id'].isin(existing_id)]

    if not new_records_df.empty:
        data_dict = new_records_df.to_dict('records')
        collection.insert_many(data_dict)
        print(f"Inserted {len(new_records_df)} new records into the MongoDB collection '{collection}'.")
        print(new_records_df[['id', 'title']].drop_duplicates())
    else:
        print("No new records to insert.")

    client.close()

def load_tasks():

    import json

    with open('plugins/keys.json', 'r', encoding='utf-8') as file:
        keys = json.load(file)

    postgres_user = keys['postgres_user']
    postgres_password = keys['postgres_password']
    mongodb_user = keys['mongodb_user']
    mongodb_password = keys['mongodb_password']

    with TaskGroup(
            group_id="load",
            tooltip="load dataframe to postgres"
    ) as group:

        load_to_postgres_task = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres,
            op_kwargs={
                'username': postgres_user,
                'password': postgres_password,
                'host': 'postgres',
                'port': '5432',
                'database': 'ohitv_database',
                'table_name': 'ohitv_request'
            }
        )

        load_to_mongodb_task = PythonOperator(
            task_id='load_to_mongodb',
            python_callable=load_to_mongodb,
            op_kwargs={
                'username': mongodb_user,
                'password': mongodb_password,
                'host': 'mongodb',
                'port': '27017',
                'database': 'ohitv',
                'collection': 'ohitv_request'
            }
        )

        [load_to_postgres_task,load_to_mongodb_task]

        return group