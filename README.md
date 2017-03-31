# ktee
Like *nix tee but pipes stdin to Apache Kafka

## Prerequisites
* [Snappy compression library](https://github.com/google/snappy)

## Build
```
cargo build --release
```

### Cross-compiling
Use Rustup. [https://blog.rust-lang.org/2016/05/13/rustup.html](https://blog.rust-lang.org/2016/05/13/rustup.html)

## Usage
```
$ target/debug/ktee -h
ktee 0.1.0
tee for Apache Kafka

USAGE:
    ktee [OPTIONS] --topic <TOPIC>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --broker <BROKER>    Kafka broker
    -t, --topic <TOPIC>      Kafka topic
```

## License
All aspects of this software are distributed under the MIT License. See LICENSE file for full license text.
