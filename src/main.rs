extern crate clap;
extern crate kafka;

use clap::{Arg, App};
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use std::time::Duration;
use std::io;
use std::io::prelude::*;

fn main() {
    let matches = App::new("ktee")
                      .version("0.1.0")
                      .author("Xavier Stevens <xavier@mystobreak.com>")
                      .about("tee for Kafka")
                      .arg(Arg::with_name("broker")
                               .short("b")
                               .long("broker")
                               .value_name("BROKER")
                               .help("Kafka broker")
                               .takes_value(true)
                               .required(true))
                      .arg(Arg::with_name("topic")
                               .short("t")
                               .long("topic")
                               .value_name("TOPIC")
                               .help("Kafka topic")
                               .takes_value(true)
                               .required(true))
                      .get_matches();

    let broker = matches.value_of("broker").unwrap();
    let topic = matches.value_of("topic").unwrap();

    let mut client = KafkaClient::new(vec![broker.to_owned()]);
    let meta_res = client.load_metadata_all();
    if let Some(err) = meta_res.err() {
        println!("Error fetching metadata: {}", err);
        return;
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        if line.is_ok() {
            let b = line.unwrap().into_bytes();
            let req = vec![ProduceMessage::new(topic, 0, None, Some(&b))];
            let res = client.produce_messages(RequiredAcks::One, Duration::from_millis(0), req);
            if let Some(err) = res.err() {
                println!("Error sending message: {}", err);
            }
        }
    }
}
