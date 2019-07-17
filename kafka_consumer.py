# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 17:25:36 2019

@author: Thangarajan
"""
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

consumer = KafkaConsumer(
    'tweet',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: str(x.decode('utf-8')))

for _ in consumer:
    print('Consuming Kafka Data by {}'.format(datetime.now()))

    try:    
            data = json.loads(_.value)
            data['timestamp'] = datetime.now()

            es.index(index='streaming', doc_type='tweet',      body=data)
    except Exception:
        pass
