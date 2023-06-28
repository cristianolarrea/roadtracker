from kafka.admin import KafkaAdminClient, NewTopic
import time
#check if broker is available before running this script
#connects to kafka running on docker
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers="kafka:9092",
        client_id='test'
    )

    topic_list = []
    topic_list.append(NewTopic(name="sensor-data", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list)

    admin_client.close()
except:
    admin_client.close()