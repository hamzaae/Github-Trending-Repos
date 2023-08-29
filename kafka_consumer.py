from kafka import KafkaConsumer
from json import loads
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient


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





def delete_blob_content(blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Upload an empty string to the blob, effectively deleting its content
    blob_client.upload_blob("", overwrite=True)
    
    print(f"Content of '{blob_name}' deleted.")

# Function to upload CSV data to Blob Storage
def upload_csv_to_blob(csv_data, blob_name):
    csv_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    try:
        # Download the existing CSV content of the blob
        existing_csv_data = csv_blob_client.download_blob().readall().decode("utf-8")

        if (existing_csv_data == ''):
            # If the blob is empty, initialize with header
            header = "name,owner,description,language,stars,forks,score,date_score"
            updated_csv_data = header + '\n' + '\n'.join(csv_data)
        else:
            # Append new data to existing CSV data
            updated_csv_data = existing_csv_data + '\n' + '\n'.join(csv_data)
    except:
        # If the blob doesn't exist or can't be read, initialize with header
        header = "name,owner,description,language,stars,forks,score,date_score"
        updated_csv_data = header + '\n' + '\n'.join(csv_data)

    # Upload the updated CSV data to the blob, overwriting the existing blob
    csv_blob_client.upload_blob(updated_csv_data, overwrite=True)

    print("Trending repos data appended and saved as CSV to Azure Storage.")


# Save CSV data
csv_data = []
delete_blob_content("2day_trending_repos_input.csv")

for message in consumer:
    trending_repos = message.value

    # Prepare CSV data for this message
    csv_data.append(','.join(map(str, trending_repos.values())))

    # Upload CSV data to Blob Storage
    blob_name = "trending_repos_input.csv"
    blob_name_2day = "2day_trending_repos_input.csv"
    upload_csv_to_blob(csv_data, blob_name)
    upload_csv_to_blob(csv_data, blob_name_2day)
    
    # Clear the csv_data list to avoid duplication
    csv_data = []

