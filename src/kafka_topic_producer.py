import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return super().default(o)

producer = KafkaProducer(
    bootstrap_servers=['192.168.64.5:9092'], 
    value_serializer=lambda v: json.dumps(v, cls=JSONEncoder).encode('utf-8')
)
client = MongoClient("mongodb://192.168.64.5:27017")
db = client['yelp']
collections = db.list_collection_names()
#Streaming multiple topic simultaneously
while True:
    for collection_name in collections:
        collection = db[collection_name]
        for record in collection.find().limit(100):
            producer.send(f'yelp-{collection_name}', record)
        producer.flush() 
    time.sleep(5)
