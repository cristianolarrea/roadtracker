from kafka.admin import KafkaAdminClient, NewTopic
import time
#check if broker is available before running this script

#connects to kafka running on docker
while True:
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:29092",
            client_id='test'
        )

        topic_list = []
        topic_list.append(NewTopic(name="sensor-data", num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Topic created successfully")
        break
    except:
        print("Broker not available, retrying in 5 seconds")
        time.sleep(1)
        continue

admin_client.close()
