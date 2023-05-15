//# https://blog.logrocket.com/building-rust-microservices-apache-kafka/#getting-started-kafka

use kafka;
use std::str;

fn main() {
    println!("Call kafka_producer ...");
    kafka_producer();
    println!("Call kafka_consumer ...");
    kafka_consumer();
    println!("The END.");
}

fn kafka_consumer() {
    let hosts = vec!["localhost:9092".to_owned()];

    let mut consumer = kafka::consumer::Consumer::from_hosts(hosts)
        .with_topic("topic-name".to_owned())
        //.with_fallback_offset(kafka::consumer::FetchOffset::Latest)
        .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
        .create()
        .unwrap();
    let mut count_messages = 0;
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                // If the consumer receives an event, this block is executed
                count_messages += 1;
                println!("count:{count_messages} >> {:?}", str::from_utf8(m.value).unwrap());
            }

            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();
    }
}

fn kafka_producer() {
    let hosts = vec!["localhost:9092".to_owned()];

    let mut producer = kafka::producer::Producer::from_hosts(hosts)
        .create()
        .unwrap();

    for i in 0..10 {
        let buf = format!("{i}");
        producer
            .send(&kafka::producer::Record::from_value(
                "topic-name",
                buf.as_bytes(),
            ))
            .unwrap();
        println!("Sent: {i}");
    }
}
