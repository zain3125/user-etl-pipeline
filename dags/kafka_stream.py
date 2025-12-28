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
    data['id'] = uuid.uuid4()
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

# print(format_data(get_data()))

def stream_data():
    # Create kafka producer, connect to server, and max waiting 5s
    producer = KafkaProducer(bootstrap_server=['broker:29092'], max_block_ms=5000)

    # Send data for one minute
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            user_data = (format_data(get_data()))
            
            producer.send('user_created', json.dumps(user_data).encode('utf-8'))
        except Exception as e:
            logging.error(f"error: {e}")
            continue
