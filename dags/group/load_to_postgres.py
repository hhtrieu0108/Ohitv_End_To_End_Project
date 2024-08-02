from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def create_client():
    from minio import Minio
    import json

    with open('plugins/keys.json', 'r') as file:
        keys = json.load(file)
    access_key = keys['access_key']
    secret_key = keys['secret_key']
    client = Minio(
        "minio:9000",  # Replace with your MinIO server address
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    return client
def create_bucket_minio(client,minio_bucket):

    from minio.error import S3Error
    try:
        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)
        else:
            print(f"Bucket '{minio_bucket}' already exists.")
    except S3Error as e:
        print(f"Error occurred: {e}")

def load_to_database(username,password,host,database,port,table_name):
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


def load_tasks():
    with TaskGroup(
            group_id="load",
            tooltip="load dataframe to postgres"
    ) as group:
        load_to_postgres_task = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_database,
            op_kwargs={
                'username': 'airflow',
                'password': 'airflow',
                'host': 'postgres',
                'port': '5432',
                'database': 'airflow',
                'table_name': 'ohitv_request'
            }
        )

        return group