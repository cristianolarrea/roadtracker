import redshift_connector

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

#create table for sensor data

# table structure is as follows:
# Placa > String  > Not Null | X > Integer > Not Null | Y > Integer > Not Null

cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS sensor_data (placa CHAR(7) NOT NULL, x INT NOT NULL, y INT NOT NULL, data_hora TIMESTAMP NOT NULL)')

conn.commit()
conn.close()
