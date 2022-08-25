#!/usr/bin/env python

import argparse
from binance import ThreadedWebsocketManager
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Read schema from file
with open('schema.txt', 'r') as f:
    schema_str = f.read()

def flatten_json(nested_json):
    """
    Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def main(args):

    topic = args.topic
    symbols = args.symbols

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}
    producer = SerializingProducer(producer_conf)

    def handle_socket_message(msg):

        flatten = flatten_json(msg)
        flatten_keys = list(flatten.keys())
        
        flatten['event_type'] = flatten['e']
        flatten['event_time'] = flatten['E']
        flatten['event_symbol'] = flatten['s']
        flatten['kline_start_time'] = flatten['k_t']
        flatten['kline_end_time'] = flatten['k_T']
        flatten['kline_symbol'] = flatten['k_s']
        flatten['kline_interval'] = flatten['k_i']
        flatten['first_trade_id'] = flatten['k_f']
        flatten['last_trade_id'] = flatten['k_L']
        flatten['open_price'] = flatten['k_o']
        flatten['close_price'] = flatten['k_c']
        flatten['high_price'] = flatten['k_h']
        flatten['low_price'] = flatten['k_l']
        flatten['volume'] = flatten['k_v']
        flatten['number_of_trades'] = flatten['k_n']
        flatten['check_if_final_bar'] = flatten['k_x']
        flatten['quote_volume'] = flatten['k_q']
        flatten['active_buy_volume'] = flatten['k_V']
        flatten['active_buy_quote_volume'] = flatten['k_Q']
        
        for keys in flatten_keys:
            del flatten[keys]
        
        producer.produce(topic=topic, key=symbol, value=flatten)
        producer.flush()

    twm = ThreadedWebsocketManager()
    twm.start()
    
    for symbol in symbols:
        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    twm.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Avro flattened Serializing Producer")
    parser.add_argument('-b', dest="bootstrap_servers",
                        default="kafka.kafka.svc.cluster.local:9092", help="Bootstrap servers")
    parser.add_argument('-r', dest="schema_registry",
                        default="http://schema-registry.kafka.svc.cluster.local:8085", help="Schema registry url")
    parser.add_argument('-t', dest="topic", default="test", help="Topic")
    parser.add_argument('-s', dest="symbols", nargs='*', default=["BTCUSDT"], help="Symbol")

    main(parser.parse_args())
