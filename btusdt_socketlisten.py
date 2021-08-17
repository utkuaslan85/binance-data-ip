from kafka import KafkaProducer
from binance import ThreadedWebsocketManager
import json
from s3fs.core import S3FileSystem


# s3fs client
s3 = S3FileSystem(
    anon=False,
    key='access-key',
    secret='secret-key',
    use_ssl=False,
    client_kwargs={'endpoint_url': 'http://10.152.183.240:9000'})


# Binance client
with s3.open('repo/binance/binance_cred.txt', 'r') as f:
    cred = json.loads(f.read())
api_key = cred["API_KEY"]
api_secret = cred["SECRET_KEY"]
client = Client(api_key=api_key, api_secret=api_secret)


# Kafka Client
producer = KafkaProducer(bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))


def main():

    symbol = 'BTCUSDT'
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)

    twm.start()

    def handle_socket_message(msg):

        producer.send('test', {symbol : msg})

    twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    twm.join()


if __name__ == "__main__":
    main()