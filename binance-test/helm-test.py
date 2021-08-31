#!/usr/bin/env python

import json
import argparse
from s3fs.core import S3FileSystem
from binance import ThreadedWebsocketManager
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Read schema from file
with open('schema.txt', 'r') as f:
    schema_str = f.read()


# s3 params
with open('s3_params.txt', 'r') as f:
    s3_params = json.loads(f.read())
s3_key = s3_params['access-key']
s3_secret = s3_params['secret-key']
s3_url = s3_params['endpoint_url']
s3_binance_cred_path = s3_params['cred_path']


# s3fs client
s3 = S3FileSystem(
     anon=False,
     key=s3_key,
     secret=s3_secret,
     use_ssl=False,
     client_kwargs={'endpoint_url': s3_url})


# Binance client
with s3.open(s3_binance_cred_path, 'r') as f:
    cred = json.loads(f.read())
api_key = cred["API_KEY"]
api_secret = cred["SECRET_KEY"]


def main(args):

    topic = args.topic
    symbol = args.symbol

    schema_registry_conf = {'url': args.schema_registry}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)

    twm.start()

    def handle_socket_message(msg):

        producer.produce(topic=topic, key=symbol, value=msg)
        producer.flush()
        print(type(msg))

    twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    twm.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers",
                        default="kafka.kafka.svc.cluster.local:9092", help="Bootstrap servers")
    parser.add_argument('-r', dest="schema_registry",
                        default="http://10.152.183.68:8085", help="Schema registry url")
    parser.add_argument('-t', dest="topic", default="test", help="Topic")
    parser.add_argument('-s', dest="symbol", default="BTCUSDT", help="Symbol")

    main(parser.parse_args())
