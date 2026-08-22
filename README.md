# Distributed Messaging System

A Kafka compatible message broker written in Go. This project uses Kafka's wire protocol, so existing Kafka clients such as kcat can communicate with the broker without a custom client library. It is currently aimed to be primarily a learning project for exploring how to design an append only storage and distributed systems. However I've made real optimizations, including with profilers and have achieved pretty decent benchmark results, which will be published here along with a more detailed look once everything is done.

## Current state

The broker currently includes:

- Kafka compatible APIs for producing, fetching and discovering topics
- persistent partitioned log storage
- leader follower replication and recovery
- a Raft based controller for broker and partition metadata
- compatibility with standard Kafka clients such as kcat

The controller and replica fetching paths are implemented and tested, but the default `cmd/broker` entry point currently runs as a standalone broker. Multiprocess startup and controller discovery are still being integrated and should be done in a couple weeks.

## Project structure

```text
cmd/broker          broker executable
internal/broker     request handling, partitions and replica fetching
internal/controller Raft controller and metadata state machine
internal/log        segmented log, indexes and leader epoch persistence
internal/protocol   Kafka protocol codecs
internal/server     TCP server and response writing
```

## Running the broker

```bash
go run ./cmd/broker \
  -broker-id 1 \
  -host localhost \
  -port 9092 \
  -log-dir /tmp/msgbroker-data
```

The standalone broker can auto create a one partition topic when it receives a metadata request for an unknown topic.

## Testing with kcat

Inspect broker metadata:

```bash
kcat -b localhost:9092 -L
```

Produce uncompressed, nonidempotent records:

```bash
printf 'first\nsecond\nthird\n' |
  kcat -P \
    -b localhost:9092 \
    -t messages \
    -p 0 \
    -X enable.idempotence=false \
    -X acks=1 \
    -X compression.codec=none
```

Read them directly from partition zero:

```bash
kcat -C \
  -b localhost:9092 \
  -t messages \
  -p 0 \
  -o beginning \
  -e \
  -q \
  -f '%o:%s\n'
```

Expected output:

```text
0:first
1:second
2:third
```

## Tests

I've not committed the tests yet, they will be on the repository in a few weeks once the project core is finished.

```bash
go vet ./...
go test ./...
go test -race ./internal/controller ./internal/broker ./internal/log
```

The test suite covers the storage format, epoch recovery, truncation, long polling, Raft state replication, controller snapshots, leader fencing and divergent follower reconciliation.

## Limitations

This is not a complete Kafka implementation. Consumer groups, transactions, idempotent producers, administrative topic APIs, SASL and TLS are not implemented yet. The project is not intended for production use ( atleast yet :) ).

More detailed architecture, correctness and benchmark documentation will be added soon.
