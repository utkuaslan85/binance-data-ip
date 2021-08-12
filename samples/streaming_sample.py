import time
from binance import ThreadedWebsocketManager
from binance_config import *
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'])

api_key = API_KEY
api_secret = SECRET_KEY

def main():

    symbol = 'BNBBTC'

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    # start is required to initialise its internal loop
    twm.start()

    def handle_socket_message(msg):
        print(f"message type: {msg['e']}")
        print(msg)

    kline_stream_name = twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    # multiple sockets can be started
    depth_stream_name = twm.start_depth_socket(callback=handle_socket_message, symbol=symbol)

    # or a multiplex socket can be started like this
    # see Binance docs for stream names
    streams = ['bnbbtc@miniTicker', 'bnbbtc@bookTicker']
    multiplex_stream_name = twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)

    payload = twm.join()
    producer.send('test', key=bytearray('test', 'utf-8'), value=bytearray(payload, 'utf-8'))

if __name__ == "__main__":
   main()