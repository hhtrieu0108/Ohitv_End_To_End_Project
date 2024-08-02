from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def convert_to_dataframe(ti):
    import pandas as pd
    df_value = ti.xcom_pull(key='crawl',task_ids='crawling.crawl_all')
    df = pd.DataFrame(df_value,columns=['title','links','date','rating','quality','genre','short_description'])
    df['date'] = df['date'].astype(str)
    df_list = df.to_dict(orient='records')
    ti.xcom_push(key='dataframe', value=df_list)

def processing(ti):
    import pandas as pd
    df_list = ti.xcom_pull(key='dataframe',task_ids='processing.convert_to_dataframe')
    df = pd.DataFrame(df_list)
    new_df = df.copy()
    new_df['date'] = new_df['date'].astype(str)
    new_df_list = new_df.to_dict(orient='records')
    ti.xcom_push(key='processed_df',value=new_df_list)


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