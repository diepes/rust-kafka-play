# Rust Kafka - Pieter 2023-05-15

Simple example of using Kafka from rust.

Code from: [https://blog.logrocket.com/building-rust-microservices-apache-kafka/#getting-started-kafka]

1. Setup docker kafka server. See "Docker kafka" below to run and create initial topic
2. cargo run
    1. producer sends 10 new events (data) to kafka
    2. consumer retrieves all events for the topic.
    3. Ctrl-c to end, and run again to see consumer grow.

## Docker kafka

     docker run -p 2181:2181 -p 9092:9092 --name kafka-docker-container --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 spotify/kafka

     Inside container
     /opt/kafka*/bin/kafka-topics.sh --topic topic-name --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1
