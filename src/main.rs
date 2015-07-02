extern crate docopt;
extern crate rustc_serialize;
extern crate kafka;

use docopt::Docopt;
use kafka::client::KafkaClient;
use std::io;
use std::io::prelude::*;

// usage string
static USAGE: &'static str = "
ktee - tee for Kafka.

Usage:
    ktee [-b BROKER] -t TOPIC
    ktee (-h | --help)

Options:
    -h, --help                  Show this message.
    -b BROKER, --broker=BROKER  Kafka broker [default: localhost:9092].
    -t TOPIC, --topic=TOPIC     Kafka topic.
";

#[derive(RustcDecodable, Debug)]
struct Args {
    flag_broker: String,
    flag_topic: String,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    let mut client = KafkaClient::new(vec!(args.flag_broker));
    let meta_res = client.load_metadata_all();
    if meta_res.is_err() {
        match meta_res.err() {
            Some(err) => println!("Error: {}", err),
            None => println!("Non-specific error")
        }
        return;
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let res = client.send_message(
            1,                           // Required Acks
            0,                           // Timeout
            args.flag_topic.to_string(), // Topic
            line.unwrap().into_bytes()   // Message
        );
        if res.is_err() {
            match res.err() {
                Some(err) => println!("{}", err),
                None => println!("Non-specific error")
            }
        }
    }
}
