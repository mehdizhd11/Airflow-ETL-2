from ETL2.Extractor.extractor import ExtractorToRedis


extractor = ExtractorToRedis(redis_host='localhost', redis_port=6379)
