# ADR-00005: Reuse the workshop Kafka broker initially

Date: 2026-09-07

Status: Accepted

## Context

The original goal includes an optional Docker Compose KRaft cluster with two brokers. For the first implementation, the owner explicitly requested using the already-running broker from the sibling Kafka workshop and adding another broker later. The owner removed the workshop's separate second cluster.

## Decision

Default host execution to the workshop broker at `localhost:9094`, with environment overrides for other clusters. When running on the workshop Docker network, use its internal listener at `kafka1:9092`.

Keep application code in `kafka-demo` and use `kafka-workshop` as reference and current infrastructure. Create a dedicated demo topic with three partitions and replication factor one for the current single broker. Use a dedicated configurable consumer group.

Defer a repository-owned Compose file, the second broker, and the KRaft controller topology. Do not carry the removed separate-cluster setup forward as a two-broker design.

## Alternatives

- Provisioning a new cluster immediately would duplicate working infrastructure before it is needed.
- Adding the second broker during backend implementation would expand the increment into cluster design and failure behavior.

## Consequences

The current application requires external Kafka infrastructure. Host and container listener addresses differ and must be configured accordingly. The existing replication factor cannot demonstrate replica redundancy. The eventual two-broker setup remains part of the intent, but its controller quorum and failure guarantees require a future ADR.
