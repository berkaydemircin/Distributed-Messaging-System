# Distributed Messaging System

A Kafka compatible message broker written in Go. It uses Kafka's wire protocol so existing clients such as kcat can produce and fetch records without a custom client library. The project explores append only storage, broker replication and metadata coordination, with performance profiling and benchmarking of the standalone broker.

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

## Initial write benchmark

On a single standalone broker, the Kafka producer performance tool reported the following results for 100 byte records (`acks=1`, no compression or idempotency):

| Workload | Ingest throughput | Producer p99 latency |
| --- | ---: | ---: |
| One producer, one partition | 2.41M records/s (median of three runs) | 1 ms |
| Four producers, four partitions | 8.00–8.15M records/s (aggregate, two runs) | 1 ms per producer |

These runs used commit `3f74c89`, a GCP `c4-standard-48-lssd` broker and a separate `c4-highcpu-24` producer VM, with 50M records per single producer run and 100M total per four producer run. The latency is measured by the producer through acknowledgment at millisecond resolution. These short runs can be served by the OS page cache so they do not measure consumer throughput, replication yet. More sophisticated tests and benchmarks will be added soon.

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

More detailed architecture and correctness documentation will be added as the project develops.
