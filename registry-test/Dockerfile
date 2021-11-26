FROM python:3.9.8-alpine3.14
RUN apk update && \
    apk add \
    g++ \
    librdkafka-dev \
    wget \
    git
RUN pip install --upgrade pip && \
    pip install \
    fastavro \
    avro \
    confluent-kafka \
    python-binance \
    s3fs
