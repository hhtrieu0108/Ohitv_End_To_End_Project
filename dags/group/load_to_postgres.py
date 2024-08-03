from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.minio_service import create_client
from plugins.minio_service import create_bucket_minio
from pymongo import MongoClient


def load_to_postgres(username,password,host,database,port,table_name):
    """
    definition : import to postgres database and save to local an csv file
    """
    import pandas as pd
    from io import BytesIO

    minio_bucket = 'ohitv-processed'
    client = create_client()
    create_bucket_minio(client=client, minio_bucket=minio_bucket)
    processed_ohitv_object = client.get_object(minio_bucket,"ohitv_request_processed.parquet")
    processed_ohitv_df = pd.read_parquet(BytesIO(processed_ohitv_object.read()))
    from sqlalchemy import create_engine
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}" # Connect to database
    engine = create_engine(db_connection_string)
    processed_ohitv_df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)

def load_to_mongodb(username,password,database,collection,host,port):
    import pandas as pd
    from io import BytesIO

    minio_bucket = 'ohitv-processed'
    client = create_client()
    create_bucket_minio(client=client, minio_bucket=minio_bucket)
    processed_ohitv_object = client.get_object(minio_bucket,"ohitv_request_processed.parquet")
    processed_ohitv_df = pd.read_parquet(BytesIO(processed_ohitv_object.read()))
    processed_ohitv_df['date'] = processed_ohitv_df['date'].fillna(value='None')
    connection_string = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
    client = MongoClient(connection_string)
    db = client[f"{database}"]
    collection = db[f"{collection}"]
    data_dict = processed_ohitv_df.to_dict('records')

    if collection.count_documents({}) == 0:  # Check if collection is empty
        collection.insert_many(data_dict)  # Insert data
        print("Data inserted into MongoDB.")
    else:
        print("Collection already exists. Data not inserted.")

    client.close()

def load_tasks():

    import json

    with open('plugins/keys.json', 'r') as file:
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
                'database': 'airflow',
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