extern crate clap;
extern crate kafka;

use clap::{Arg, App};
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use std::time::Duration;
use std::io;
use std::io::prelude::*;

fn main() {
    let args = App::new("ktee")
                   .version("0.1.0")
                   .about("tee for Apache Kafka")
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
    let broker = args.value_of("broker").unwrap();
    let topic = args.value_of("topic").unwrap();

    let mut client = KafkaClient::new(vec![broker.to_owned()]);
    let meta_res = client.load_metadata_all();
    if let Some(err) = meta_res.err() {
        println!("Error fetching metadata: {}", err);
        return;
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        match line {
            Ok(line) => {
                let req = vec![ProduceMessage::new(topic, 0, None, Some(&line.as_bytes()))];
                let res = client.produce_messages(RequiredAcks::One, Duration::from_millis(0), req);
                if let Some(err) = res.err() {
                    writeln!(&mut io::stderr(), "Error sending message: {}", err).unwrap();
                }

                let write_res = write!(&mut io::stdout(), "{}\n", &line);
                if let Some(err) = write_res.err() {
                    writeln!(&mut io::stderr(), "Error writing to stdout: {}", err).unwrap();
                }
            }
            Err(err) => writeln!(&mut io::stderr(), "Error reading from stdin: {}", err).unwrap(),
        }
    }
}
