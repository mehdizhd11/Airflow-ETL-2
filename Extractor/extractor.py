import pandas as pd
import redis
from elasticsearch import Elasticsearch


class ExtractorToRedis:

    def __init__(self, redis_host, redis_port):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


    def csv_to_redis(self, csv_path, redis_key = 'etl_data_extractor'):
        df = pd.read_csv(csv_path)
        self.redis.set(redis_key, df.to_json())
        print('csv_to_redis done')


    def mongo_to_redis(self, collection, redis_key = 'etl_data_extractor'):
        # Fetch data from MongoDB
        data = list(collection.find({}))

        # Convert the data to a DataFrame
        df = pd.DataFrame(data)


        # Ensure UTF-8 encoding
        def ensure_utf8(df):
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(
                    lambda x: x.encode('latin1').decode('utf-8', errors='replace') if isinstance(x, str) else x)
            return df


        df = ensure_utf8(df)

        # Convert DataFrame to JSON string
        json_str = df.to_json(orient='records', force_ascii=False)

        # Store the JSON string in Redis
        self.redis.set(redis_key, json_str)
        print('mongo_to_redis done')


    def elasticsearch_to_redis(self, elastic_index, redis_key = 'etl_data_extractor'):
        es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
                           basic_auth=('elastic', '0yNVDELqtj4MB2PNmnGJ'), verify_certs=False)
        res = es.search(index=elastic_index, body={"query": {"match_all": {}}})
        data = [hit['_source'] for hit in res['hits']['hits']]
        df = pd.DataFrame(data)
        self.redis.set(redis_key, df.to_json())
        print('elastic_index_to_redis done')
