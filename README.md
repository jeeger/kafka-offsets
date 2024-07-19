# offsets: A simple Kafka offset administration tool

## Features

- Print offsets for topics
- Print consumer group offsets
- Reset offsets for consumer groups and offsets

Supports profiles to be able to work with different servers, such as prod or staging. Multiple
topics and groups can be provided to reset offsets for several groups and topics at the same time.
Profile information is read from a file called "kafkaconfig.edn" in the current directory, an alternate configuration file can be provided with `-c`.

## Running

To run from a git checkout, clone the repository, install Clojure and run "clj -M:run". Since this
is a bit slow for interactive use, you can also generate a native binary by running "clj -M:uberdep;
clj -M:compile-native". This requires you to have
[GraalVM](https://www.graalvm.org/latest/docs/getting-started/) installed. This will generate
`target/offsets`, which you can then copy to your PATH.
