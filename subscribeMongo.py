from kafka import KafkaConsumer
from pymongo import MongoClient

#Creating Kafka consumer connection
consumer = KafkaConsumer('sensor-data')

MONGO_SERVER = 'mongodb://localhost:27017'
DATABASE_NAME = 'roadtracker'
COLLECTION_NAME = 'sensor-data'

client = MongoClient(MONGO_SERVER)
db = client[DATABASE_NAME]
coll = db[COLLECTION_NAME]

def transformToJson(mensagem):
    dict = {}
    data = mensagem.decode('utf-8')[:-1].split(',')
    dict['road'] = data[0]
    dict['road_speed'] = int(data[1])
    dict['road_size'] = int(data[2])
    dict['x'] = int(data[3])
    dict['y'] = int(data[4])
    dict['plate'] = data[5]
    dict['time'] = float(data[6])
    dict['direction'] = int(data[7])
    return dict

while True:
    for msg in consumer:
        coll.insert_one(transformToJson(msg.value))
