#!/usr/bin/env python

import time
import json
import requests
import argparse
from kafka import KafkaProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Read schema from file
with open('sap_schema.txt', 'r') as f:
    schema_str = f.read()


def main(args):

    topic = args.topic
    symbol = args.symbol
    """    
    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)
    """
    producer = KafkaProducer(bootstrap_servers=['kafka.kafka.svc.cluster.local:9092'])
    payload = {'HatHucre' : '1', 'Werks' : '1000'}

    while True:
        
        time.sleep(0.5)
        r = requests.post('http://andon.coskunoz.com.tr:44312/AndonScreen1/Get_Andon_Data', json=payload)
        bar = r.text
        foo = json.loads(bar)
        for ham in foo:
            #producer.produce(topic=topic, key=symbol, value=ham)
            #producer.flush()
            print(type(ham))
            producer.send(topic, key=bytearray(symbol, 'utf-8'), value=bytearray(str(ham), 'utf-8'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers",
                        default="kafka.kafka.svc.cluster.local:9092", help="Bootstrap servers")
    parser.add_argument('-r', dest="schema_registry",
                        default="http://10.152.183.68:8085", help="Schema registry url")
    parser.add_argument('-t', dest="topic", default="SAP_ANDON", help="Topic")
    parser.add_argument('-s', dest="symbol", default="SAP", help="Symbol")

    main(parser.parse_args())
