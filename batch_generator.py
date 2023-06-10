from kafka import KafkaConsumer
import boto3
import pandas as pd
import time

#Creating AWS connection
session = boto3.session.Session(profile_name='computacao-escalavel')
s3 = session.resource('s3')

#Creating Kafka consumer connection
consumer = KafkaConsumer('sensor-data')

def saveInS3(df):
    object = s3.Object('roadtracker', f'{time.time()}.parquet')
    object.put(Body=df.to_parquet(engine='pyarrow'))

THRESHOLD = 5
while True:
    df = pd.DataFrame({})
    i=0
    for msg in consumer:
        try:
            df = pd.concat([df, pd.DataFrame([msg.value])])
            i+=1
        except Exception as e:
            print(e)
        if i==THRESHOLD:
            break
    saveInS3(df)


