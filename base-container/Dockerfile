# syntax=docker/dockerfile:1
FROM python:3.8.11-alpine3.14
RUN apk add --no-cache g++ librdkafka-dev wget
RUN pip install --upgrade pip
RUN pip install fastavro avro confluent-kafka python-binance s3fs
