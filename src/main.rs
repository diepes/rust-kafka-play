//# https://blog.logrocket.com/building-rust-microservices-apache-kafka/#getting-started-kafka

use kafka;
use std::str;

const NEW_TOPIC: &str = "topic-name2";

fn main() {
    println!("Call kafka_client ...");
    kafka_client();
    println!("Call kafka_producer ...");
    kafka_producer();
    println!("Call kafka_consumer ...");
    kafka_consumer();
    println!("The END.");
}

fn kafka_client() {
    //# https://docs.rs/kafka/latest/kafka/client/struct.KafkaClient.html
    let mut client = kafka::client::KafkaClient::new(vec!["localhost:9092".to_owned()]);
    client
        .load_metadata_all()
        .expect("Error client connection to kafka");
    fn print_topics(prefix: &str, client: &kafka::client::KafkaClient) {
        for (i,topic) in client.topics().names().enumerate() {
            println!("    {prefix} kafka::client topic: {i}: {}", topic);
        }
    }
    print_topics("before", &client);
    println!("    kafka_client create topic ");
    let _ = client
        .load_metadata(&[NEW_TOPIC])
        .expect("Error kafka_client loading metadata");

    print_topics("after", &client);
    println!("     kafka_client done.");
}

fn kafka_consumer() {
    let hosts = vec!["localhost:9092".to_owned()];

    let mut consumer = kafka::consumer::Consumer::from_hosts(hosts)
        .with_topic(NEW_TOPIC.to_string())
        //.with_fallback_offset(kafka::consumer::FetchOffset::Latest)
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create()
        .expect("Error creating the consumer to {hosts}");
    let mut count_messages = 0;
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                // If the consumer receives an event, this block is executed
                count_messages += 1;
                println!(
                    "count:{count_messages} >> {:?}",
                    str::from_utf8(m.value).unwrap()
                );
            }

            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();
    }
    println!("     kafka_consumer done.");
}

fn kafka_producer() {
    let hosts = vec!["localhost:9092".to_owned()];

    let mut producer = kafka::producer::Producer::from_hosts(hosts)
        .create()
        .expect(format!("Error while creating the kafka producer").as_str());

    for i in 0..10 {
        let buf = format!("{i}");
        producer
            .send(&kafka::producer::Record::from_value(
                NEW_TOPIC,
                buf.as_bytes(),
            ))
            .expect("Error in producer sending msg to kafka. \nDid you remember to create the kafka topic?\n");
        println!("Sent: {i}");
    }
    println!("     kafka_producer done.");
}
