#!/usr/bin/env python

import argparse
from binance import ThreadedWebsocketManager
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


def main(args):

    topic = args.topic
    symbol = args.symbol

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': StringSerializer('utf_8')}

    producer = SerializingProducer(producer_conf)

    twm = ThreadedWebsocketManager()

    twm.start()

    def handle_socket_message(msg):

        producer.produce(topic=topic, key=symbol, value=str(msg))
        producer.flush()
        
    twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    twm.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="String Serializing Producer")
    parser.add_argument('-b', dest="bootstrap_servers",
                        default="kafka.kafka.svc.cluster.local:9092", help="Bootstrap servers")
    parser.add_argument('-r', dest="schema_registry",
                        default="http://schema-registry.kafka.svc.cluster.local:8085", help="Schema registry url")
    parser.add_argument('-t', dest="topic", default="test", help="Topic")
    parser.add_argument('-s', dest="symbol", default="BTCUSDT", help="Symbol")

    main(parser.parse_args())
