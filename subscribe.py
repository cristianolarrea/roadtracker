from kafka import KafkaConsumer
import redshift_connector
import pandas as pd

# {
#     "Cluster": {
#         "ClusterIdentifier": "mycluster",
#         "NodeType": "dc2.large",
#         "ClusterStatus": "creating",
#         "ClusterAvailabilityStatus": "Modifying",
#         "MasterUsername": "adminuser",
#         "DBName": "dev",
#         "AutomatedSnapshotRetentionPeriod": 1,
#         "ManualSnapshotRetentionPeriod": -1,
#         "ClusterSecurityGroups": [],
#         "VpcSecurityGroups": [
#             {
#         "DBName": "dev",
#         "AutomatedSnapshotRetentionPeriod": 1,
#         "ManualSnapshotRetentionPeriod": -1,
#         "ClusterSecurityGroups": [],
#         "VpcSecurityGroups": [
#             {
#             {
# {
#     "Cluster": {
#         "ClusterIdentifier": "mycluster",
#         "NodeType": "dc2.large",
#         "ClusterStatus": "creating",
#         "ClusterAvailabilityStatus": "Modifying",
#         "MasterUsername": "adminuser",
#         "DBName": "dev",
#         "AutomatedSnapshotRetentionPeriod": 1,
#         "ManualSnapshotRetentionPeriod": -1,
#         "ClusterSecurityGroups": [],
#         "VpcSecurityGroups": [
#             {
# {
#     "Cluster": {
#         "ClusterIdentifier": "mycluster",
#         "NodeType": "dc2.large",
#         "ClusterStatus": "creating",
#         "ClusterAvailabilityStatus": "Modifying",
#         "MasterUsername": "adminuser",
#         "DBName": "dev",
#         "AutomatedSnapshotRetentionPeriod": 1,
#         "ManualSnapshotRetentionPeriod": -1,
#         "ClusterSecurityGroups": [],
#         "VpcSecurityGroups": [
#             {
#                 "VpcSecurityGroupId": "sg-02a8a78cd02aa2a6c",
#                 "Status": "active"
#             }
#         ],
#         "ClusterParameterGroups": [
#             {
#                 "ParameterGroupName": "default.redshift-1.0",
#                 "ParameterApplyStatus": "in-sync"
#             }
#         ],
#         "ClusterSubnetGroupName": "default",
#         "VpcId": "vpc-05a99444551256133",
#         "PreferredMaintenanceWindow": "mon:04:30-mon:05:00",
#         "PendingModifiedValues": {
#             "MasterUserPassword": "****"
#         },
#         "ClusterVersion": "1.0",
#         "AllowVersionUpgrade": true,
#         "NumberOfNodes": 2,
#         "PubliclyAccessible": true,
#         "Encrypted": false,
#         "Tags": [],
#         "EnhancedVpcRouting": false,
#         "IamRoles": [],
#         "MaintenanceTrackName": "current",
#         "DeferredMaintenanceWindows": [],
#         "NextMaintenanceWindowStartTime": "2023-06-12T04:30:00+00:00"
#     }
# }

#Creating Kafka consumer connection
consumer = KafkaConsumer('sensor-data')

#takes the host name from the cluster commented above

#Creating Redshift connection
cluster_identifier = 'mycluster'
conn = redshift_connector.connect(
    iam=True,
    region='us-east-1',
    access_key_id="",
    secret_access_key="",
    session_token="",
    auth_profile="",
    db_user="",
    cluster_identifier=cluster_identifier,
    profile='computacao-escalavel'
)

def saveInRedshift(df):
    cursor = conn.cursor()
    cursor.execute(f'INSERT INTO sensor_data VALUES {df.to_string(index=False, header=False)}')
    conn.commit()

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
    saveInRedshift(df)
