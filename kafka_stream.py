from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

def get_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api/')
    response.raise_for_status()
    result = response.json()['results'][0]
    return result

def fromat_data(result):
    data = {}
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['gender'] = result['gender']
    data['address'] = str(result['location']['street']['number']+" "+result['location']['street']['name'])
    data['postcode'] = result['location']['postcode']
    data['email'] = result['email']
    data['dob'] = result['dob']['date'][:10]
    data['registered_date'] = result['registered']['date'][:10]
    data['phone'] = result['phone']
    data['username'] = result['login']['username']
    data['picture'] = result['picture']['medium']

    return data



def stream_data():
    import json
    

    



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