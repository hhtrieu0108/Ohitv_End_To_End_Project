from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.minio_service import create_client
from plugins.minio_service import create_bucket_minio

# def create_client():
#     from Crawl_Ohitv_Ariflow.plugins.minio_service import Minio
#     import json
#
#     with open('plugins/keys.json', 'r') as file:
#         keys = json.load(file)
#     access_key = keys['access_key']
#     secret_key = keys['secret_key']
#     client = Minio(
#         "minio:9000",  # Replace with your MinIO server address
#         access_key=access_key,
#         secret_key=secret_key,
#         secure=False
#     )
#
#     return client
# def create_bucket_minio(client,minio_bucket):
#
#     from Crawl_Ohitv_Ariflow.plugins.minio_service import S3Error
#     try:
#         if not client.bucket_exists(minio_bucket):
#             client.make_bucket(minio_bucket)
#         else:
#             print(f"Bucket '{minio_bucket}' already exists.")
#     except S3Error as e:
#         print(f"Error occurred: {e}")


def convert_to_dataframe(ti):
    import pandas as pd
    import io

    minio_bucket = 'ohitv-raw'
    client = create_client()
    create_bucket_minio(client=client,minio_bucket=minio_bucket)

    df_value = ti.xcom_pull(key='crawl',task_ids='crawling.crawl_all')
    df = pd.DataFrame(df_value,columns=['title','links','date','rating','quality','genre','short_description'])
    df_parquet = df.to_parquet(index=False)

    client.put_object(
        bucket_name=minio_bucket,
        object_name="ohitv_request_raw.parquet",
        data=io.BytesIO(df_parquet),
        length=len(df_parquet)
    )

def processing():
    import pandas as pd
    import io
    from io import BytesIO
    minio_bucket = 'ohitv-raw'
    client = create_client()
    create_bucket_minio(client=client,minio_bucket=minio_bucket)

    raw_ohitv_object = client.get_object(minio_bucket, "ohitv_request_raw.parquet")
    df = pd.read_parquet(BytesIO(raw_ohitv_object.read()))
    new_df = df.copy()
    new_df['date'] = pd.to_datetime(new_df['date'], format='%d/%m/%Y', errors='coerce')
    processed_df = new_df.explode('genre')
    processed_df['rating'] = processed_df['rating'].astype('float')
    processed_minio_bucket = 'ohitv-processed'
    create_bucket_minio(client=client,minio_bucket=processed_minio_bucket)

    processed_df_parquet = processed_df.to_parquet(index=False)
    client.put_object(
        bucket_name=processed_minio_bucket,
        object_name="ohitv_request_processed.parquet",
        data=io.BytesIO(processed_df_parquet),
        length=len(processed_df_parquet)
    )


def processing_tasks():
    with TaskGroup(
            group_id="processing",
            tooltip="processing dataframe"
    ) as group:
        convert_to_dataframe_task = PythonOperator(
            task_id='convert_to_dataframe',
            python_callable=convert_to_dataframe
        )

        processing_task = PythonOperator(
            task_id='processing',
            python_callable=processing
        )

        convert_to_dataframe_task >> processing_task

        return group