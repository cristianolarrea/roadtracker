import pandas as pd
from pymongo import MongoClient
import time

MONGO_URL = 'mongodb://localhost:27017'
database = 'roadtracker'
client = MongoClient(MONGO_URL)
db = client[database]

with open("times_to_compute.csv", "w") as f:
    f.write("times\n")

otherTimeStamp = 0

while True:
    try:
        coll = db["lasttimestamp"]
        df = pd.DataFrame(list(coll.find()))
        df = df.drop('_id', axis=1)

        LastTimeStamp = df['LastTimeStamp'][0]
        if LastTimeStamp != otherTimeStamp:
            print("New timestamp: ", LastTimeStamp)
            print("Other timestamp: ", otherTimeStamp)
            with open("times_to_compute.csv", "a") as f:
                f.write(str(LastTimeStamp) + "\n")
            otherTimeStamp = LastTimeStamp
    except:
        continue