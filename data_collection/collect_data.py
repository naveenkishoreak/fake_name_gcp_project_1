import requests
import json
import os
import time
from google.cloud import pubsub_v1

API_URL='https://randomuser.me/api/'
PROJECT_ID='naveen-dbt-project'
TOPIC_ID='fake_name_topic'
FETCH_INTERVAL=60

# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/naveenkishorek/Downloads/key.json"


# Initialize pub/sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


# Function to fetch data
def fetch_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    
    data = response.json()
    result = data['results'][0]
    useful_info = {
        "name": f"{result['name']['title']} {result['name']['first']} {result['name']['last']}",
        "location": f"{result['location']['street']['number']} {result['location']['street']['name']}, {result['location']['city']}, {result['location']['state']}, {result['location']['country']}, {result['location']['postcode']}",
        "email": result['email'],
        "phone": result['phone'],
        "cell": result['cell'],
        "picture": result['picture']['large']
    }
    return useful_info

# publish data to pub/Sub
def publish_data(data):
    data_json = json.dumps(data)
    data_bytes = data_json.encode('utf-8')
    future = publisher.publish(topic_path, data_bytes)
    print(f'Published message ID: {future.result()}')
    

def main():
    while True:
        try:
            data = fetch_data()
            publish_data(data)
            print("Waiting for next fetch...")
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            print(f'Error: {e}')
            time.sleep(FETCH_INTERVAL)
            
if __name__ == '__main__':
    main()


