# ADR-00004: Use configured topics and string records initially

Date: 2026-09-07

Status: Accepted

## Context

The first backend should make sending and observing Kafka messages easy while supporting an existing, configurable cluster. Schema infrastructure and unrestricted topic administration are unnecessary for this increment.

## Decision

Configure a topic allowlist and a default producer topic at startup. Validate that the default belongs to the list. Subscribe the Kafka listener to this list; a WebSocket URL selects an allowed topic for that viewer. Reject unconfigured topics.

Require topics to exist before startup and disable consumer-driven topic auto-creation. Topic creation is an explicit setup step, not an application API.

Use Kafka string serializers and deserializers for keys and values. The HTTP API requires a string value and accepts an optional string key and topic. Preserve consumed tombstones as null values and represent header bytes as Base64 in an ordered list, retaining duplicate header names.

## Alternatives

- Arbitrary per-request topics and dynamic listener creation would add lifecycle and resource-management concerns.
- Avro or another schema format would be useful for later serialization lessons but would require additional configuration and teaching material now.
- Application-managed topic creation would simplify first startup while also changing cluster state implicitly.

## Consequences

Examples can be sent with curl and inspected without Schema Registry. Topic-list changes require application reconfiguration and restart. UTF-8 string decoding is not a lossless representation of arbitrary binary record values; future binary or schema lessons need an explicit representation decision. Standard `spring.kafka.*` settings remain available for cluster connection and behavior changes.
