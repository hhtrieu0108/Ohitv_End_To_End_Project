from minio import Minio
from minio.error import S3Error

def create_client():
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
    try:
        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)
        else:
            print(f"Bucket '{minio_bucket}' already exists.")
    except S3Error as e:
        print(f"Error occurred: {e}")
