import redis
import pandas as pd
from pymongo import MongoClient


class RedisLoader:

    def __init__(self, redis_host, redis_port):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


    def to_csv(self, csv_path, redis_key):
        extracted_data_json = self.redis.get(redis_key)

        df = pd.read_json(extracted_data_json)
        df.to_csv(csv_path, index=False)

        print('Loaded to csv')


    def to_mongo(self, redis_key):
        extracted_data_json = self.redis.get(redis_key)
        df = pd.read_json(extracted_data_json)

        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017')
        db = client['Airflow']
        collection = db['ETL2']

        # Insert the data into MongoDB
        collection.insert_many(df.to_dict('records'))

        print('Loaded to mongo')
