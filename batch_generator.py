from kafka import KafkaConsumer
consumer = KafkaConsumer('sensor-data')

while True:
    for msg in consumer:
        print(msg)
#%%
