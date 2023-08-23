from kafka import KafkaConsumer
from json import loads
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
import json
import datetime  

# IP value
load_dotenv()
ip = os.getenv("IP")

# Create a Kafka consumer
consumer = KafkaConsumer(
    'githubTopic',
    bootstrap_servers=[f'{ip}:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Define blob details
account_name = os.getenv("USERNAME")
container_name = os.getenv("CONTAINER_NAME")
connection_string = os.getenv("CONN_STRING")

# Create a BlobServiceClient using the Azure Storage connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# last update
# save all data
for message in consumer:
    trending_repos = message.value

    blob_name = "trending_repos_input.json"

    # Create a BlobClient to get the existing blob's content
    existing_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    try:
        # Download the existing content of the blob
        existing_blob_data = existing_blob_client.download_blob().readall()

        # Convert the existing content from bytes to a string (assuming it's a JSON object)
        existing_json_data = existing_blob_data.decode("utf-8")

        # Convert existing JSON data to a Python dictionary
        existing_data_dict = json.loads(existing_json_data)
    except:
        # If the blob doesn't exist or can't be read, initialize with an empty dictionary
        existing_data_dict = {}

    # Generate a new unique key based on the current date and time
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trending_repos_with_timestamp = {current_datetime: trending_repos}

    # Merge the new data with the existing data
    updated_data_dict = {**existing_data_dict, **trending_repos_with_timestamp}

    # Convert the updated dictionary back to a JSON string
    updated_json_data = json.dumps(updated_data_dict, indent=4)  # Indent for better readability

    # Upload the updated JSON string to the blob, overwriting the existing blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(updated_json_data, overwrite=True)

    print("Trending repos data appended and saved to Azure Storage.")

    blob_name = "2day_trending_repos_input.json"

    # Convert the new data to a dictionary with a single key for the current date
    current_day = datetime.datetime.now().strftime("%Y-%m-%d")
    new_data = {current_day: trending_repos}

    # Convert the new data dictionary to a JSON string
    new_json_data = json.dumps(new_data, indent=4)  # Indent for better readability

    # Upload the new JSON string to the blob, overwriting the existing blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(new_json_data, overwrite=True)
    print("Trending repos data added and saved to Azure Storage.")

# save 2days data

for message in consumer:
    trending_repos = message.value

    blob_name = "2day_trending_repos_input.json"

    # Convert the new data to a dictionary with a single key for the current date
    current_day = datetime.datetime.now().strftime("%Y-%m-%d")
    new_data = {current_day: trending_repos}

    # Convert the new data dictionary to a JSON string
    new_json_data = json.dumps(new_data, indent=4)  # Indent for better readability

    # Upload the new JSON string to the blob, overwriting the existing blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(new_json_data, overwrite=True)

    print("Trending repos data updated and saved to Azure Storage.")
