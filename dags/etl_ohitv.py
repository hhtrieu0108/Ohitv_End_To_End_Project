from airflow import DAG
from group.crawl_ohitv import crawl_tasks
from group.transform_data import processing_tasks
from dags.group.load_to_db import load_tasks
from datetime import datetime

with DAG(
        dag_id='ohitv_pipeline',
        start_date=datetime(year=2024, 
                            month=9, 
                            day=26),
        schedule_interval='30 9 * * *',
        catchup=False) as dag:

    crawl = crawl_tasks()

    processing = processing_tasks()

    load = load_tasks()

    crawl >> processing >> load