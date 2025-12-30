from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from kafka import KafkaProducer
import logging
import requests
import time
import uuid

def get_data():
    user_data = requests.get("https://randomuser.me/api/")
    user_data = user_data.json()
    user_data = user_data['results'][0]
    return user_data

def format_data(user_data):
    data = {}
    location = user_data['location']
    data['id'] = str(uuid.uuid4()) 
    data['first_name'] = user_data['name']['first']
    data['last_name'] = user_data['name']['last']
    data['gender'] = user_data['gender']
    data['addusers'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = user_data['email']
    data['username'] = user_data['login']['username']
    data['dob'] = user_data['dob']['date']
    data['registered_date'] = user_data['registered']['date']
    data['phone'] = user_data['phone']
    data['picture'] = user_data['picture']['medium']
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            user_data = format_data(get_data())
            # نستخدم التوبيك باسم 'user_created'
            producer.send('user_created', json.dumps(user_data).encode('utf-8'))
        except Exception as e:
            logging.error(f"error: {e}")
            continue

default_args = {
    'owner': 'zain',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
