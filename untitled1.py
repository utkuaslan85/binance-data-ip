from kafka import KafkaProducer
from binance_config import *
from binance import ThreadedWebsocketManager

# Binance client
api_key = "wtqoMQp21dTGKL4qrZeHaxv0UZETf55sHOqaWLgu6wAKfmB3N7hTBPjGoMFpHAv3"
api_secret = "v1YKCDgI11Tl7czwUUgAfBvDWaZjg12hiN3HTMVen8EqDdZuVJxPiPZ3scKW1pBa"
twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)

# Kafka Client
producer = KafkaProducer(bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

symbol = 'BTCUSD'

def handle_socket_message(msg):

    producer.send('test', {symbol : msg})
    print(msg)

twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

twm.join()