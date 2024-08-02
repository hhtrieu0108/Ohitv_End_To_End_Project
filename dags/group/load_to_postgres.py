from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def load_to_database(ti,username,password,host,database,port,table_name):
    """
    definition : import to postgres database and save to local an csv file
    """
    import pandas as pd
    df_list = ti.xcom_pull(key='processed_df',task_ids='processing.processing')
    df = pd.DataFrame(df_list)
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce')
    df['date'] = df['date'].astype(str)
    df['genre'] = df['genre'].astype(str)
    df['genre'] = df['genre'].str.split(', ')

    # Explode the list into separate rows
    df = df.explode('genre')

    df['genre'] = df['genre'].str.replace('[', '')
    df['genre'] = df['genre'].str.replace(']', '')
    from sqlalchemy import create_engine
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}" # Connect to database
    engine = create_engine(db_connection_string)
    df.to_csv('ohitv_film_requests.csv',index=False)
    df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)


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