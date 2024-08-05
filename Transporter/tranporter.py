import pandas as pd
import redis


class TransportRedis:

    def __init__(self, redis_host, redis_port):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


    def slice_by_row(self, num, redis_key = 'etl_data_extractor', redis_dest = 'etl_data_transporter'):
        extracted_data_json = self.redis.get(redis_key)

        df = pd.read_json(extracted_data_json)

        # Get the number of rows
        num_rows = len(df)

        # Calculate the point
        point = num_rows // num

        # Slice the DataFrame to get the first half
        df_first_half = df.iloc[:point]

        self.redis.set(redis_dest, df_first_half.to_json())

        print('Dataframe sliced by Row')


    def slice_by_column(self, num, redis_key = 'etl_data_extractor', redis_dest = 'etl_data_transporter'):
        extracted_data_json = self.redis.get(redis_key)
        df = pd.read_json(extracted_data_json)
        num_rows = len(df.columns)
        point = num_rows // num
        df_first_half = df.iloc[:, :point]
        self.redis.set(redis_dest, df_first_half.to_json())
        print('Dataframe sliced by Column')
