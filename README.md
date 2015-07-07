# ktee
Like *nix tee but pipes stdin to Apache Kafka

## Prerequisites
* [Snappy compression library](https://github.com/google/snappy)

## Build
On Mac OS X it seems that Rust doesn't have /usr/local/lib in its linker search path(s). In general its not recommended to put things like _/usr/local/lib_ which likely contain homebrew compiled libs in DYLD_LIBRARY_PATH. This is because they might conflict with Mac's native libs. So export DYLD_FALLBACK_LIBRARY_PATH instead like so:

```
export DYLD_FALLBACK_LIBRARY_PATH=/usr/local/lib
```

ktee's build script (build.rs) picks this up if available and adds it to the link search path(s).

Assuming did everything above you should just be able to:

```
cargo build
```

### Cross-compiling
COMING SOON!

## Usage
```
$ ./target/debug/ktee -h
ktee - tee for Kafka.

Usage:
    ktee [-b BROKER] -t TOPIC
    ktee (-h | --help)

Options:
    -h, --help                  Show this message.
    -b BROKER, --broker=BROKER  Kafka broker [default: localhost:9092].
    -t TOPIC, --topic=TOPIC     Kafka topic.
```

## License
All aspects of this software are distributed under the MIT License. See LICENSE file for full license text.
