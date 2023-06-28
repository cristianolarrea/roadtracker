from confluent_kafka import Consumer
import redshift_connector
import pandas as pd

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'road-sensor',
})

consumer.subscribe(['sensor-data'])

#Creating Redshift connection
cluster_identifier = 'roadtracker'
conn = redshift_connector.connect(
    host='roadtracker.cqgyzrqagvgs.us-east-1.redshift.amazonaws.com',
    port=5439,
    user='admin',
    password='roadTracker1',
    database='road-tracker',
    cluster_identifier=cluster_identifier
)

cursor = conn.cursor()
try:
    while True:
        #create batches of 100.000 messages or .5 seconds
        # msg_0 = consumer.consume(1000, timeout=3.0)
        THRESHOLD = 100000
        TIMEOUT = 0.5

        def partMessages(message):
            val = message.value()
            #remove the b' and ' from the message
            #attention: the message is in bytes format
            #adds ' to each entry
            val = str(val)[2:-3].split(",")
            val = list(map(lambda x: "'"+x+"'", val))
            val = ",".join(val)
            val = "("+val+"),"
            return val

        messages = consumer.consume(THRESHOLD, timeout=TIMEOUT)
        if len(messages) == 0:
            continue
        messages = list(map(partMessages, messages))

        query = f'INSERT INTO sensor_data VALUES {"".join(messages[:-1])}' + messages[-1][:-1]
        cursor.execute(query)
        conn.commit()
except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    conn.close()