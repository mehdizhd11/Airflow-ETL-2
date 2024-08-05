from ETL2.Transporter.tranporter import TransportRedis


transporter = TransportRedis(redis_host='localhost', redis_port=6379)
