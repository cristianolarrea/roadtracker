import redshift_connector

cluster_identifier = 'mycluster'
#Creating Redshift connection
conn = redshift_connector.connect(
    iam=True,
    region='us-east-1',
    access_key_id="",
    secret_access_key="",
    session_token="",
    auth_profile="",
    db_user="",
)

#create table for sensor data

# table structure is as follows:
# Placa > String  > Not Null | X > Integer > Not Null | Y > Integer > Not Null

cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS sensor_data (placa CHAR(7) NOT NULL, x INT NOT NULL, y INT NOT NULL, data_hora TIMESTAMP NOT NULL)')

conn.commit()
conn.close()
