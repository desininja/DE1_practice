import sys
from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch
import time
import json
import pygeohash as pgh

es = Elasticsearch(["23.20.131.88"], port=9200)

# To consume messages
consumer = KafkaConsumer('surge_pricing_demo_output', group_id="surge_pricing_demo_output_group",
                          auto_commit_interval_ms=30 * 1000,
                          auto_offset_reset='smallest',
                          bootstrap_servers=['localhost:9092'])
esid = 0

for message in consumer:
    time.sleep(1)
    esid += 1
    if esid % 1000 == 0:
      print(esid)
   

    msg = json.loads(message.value)
    if 'surge/ride' not in msg or 'window' not in msg or 'geohash' not in msg:
      continue

    geohash = pgh.decode(msg['geohash'])
    msg = {
                              "geohash": str(geohash[0])+"," + str(geohash[1]),  
                              "surge": msg['surge/ride'], 
                              "ts": msg['window']['start']
          
                                       
         }

    print(msg)
    

    index = 'rides_demo'

    if 'schema' in msg:
      esid = 1
      print("Switching to Index", index)
      if es.indices.exists(index=index):
        print(es.indices.delete(index=index))
      print(es.indices.create(index=index ))
      print(es.indices.put_mapping(index=index, doc_type= msg['doc_type'], body=msg['schema']['mappings'] ))

    es.index(index=index, doc_type= 'surge', id=esid, body=msg)
