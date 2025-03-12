from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook


def stream_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api/')
    response.raise_for_status()
    data = response.json()['results'][0]
    print(json.dumps(data,indent=1))

    



# with DAG(
#     dag_id='kafka_stream',
#     start_date=datetime(2025,1,1),
#     schedule='@daily',
#     catchup=False,
#     tags=['kafka_stream']
# ) as dag:
    
#     stream_data_kafka = PythonOperator(
#         taks_id='stream_data_kafka',
#         python_callable=stream_data

#     )

stream_data()