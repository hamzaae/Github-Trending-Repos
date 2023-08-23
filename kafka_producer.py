# pip install kafka-python
import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json
from dotenv import load_dotenv
import os
from github_api import extractTrendingRepos

# IP value
load_dotenv()
ip = os.getenv("IP")

producer = KafkaProducer(bootstrap_servers=[f'{ip}:9092'], 
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

trending_repos = extractTrendingRepos()

for repo in trending_repos:
    producer.send('githubTopic', value=repo)
    sleep(1)
producer.flush() #clear data from kafka server