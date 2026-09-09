# ADR-00003: Decouple Kafka consumption from viewers

Date: 2026-09-07

Status: Accepted

## Context

The owner requires continued consumption when no WebSockets are active, with those records logged and discarded from the delivery pipeline. Browser navigation and slow viewers should not change or obstruct the Kafka behavior being taught.

## Decision

Run a listener for the configured topics independently of WebSocket connections. Fan each consumed record out to viewers of that topic. If there are none, log the record and return normally so batch offset commits continue.

Use a bounded queue and dedicated sender thread for each viewer. The initial limits are 64 queued events, 256 KiB per outgoing event, and 32 viewers. Disconnect an overflowing viewer rather than blocking Kafka consumption. Bound embedded Tomcat's blocking socket writes with a five-second timeout.

Use container-managed batch commits with Kafka auto-commit disabled. Do not wait for browser acknowledgment or retain a browser replay buffer. Run a single backend instance for the initial use case.

## Alternatives

- A Kafka consumer per WebSocket would tie group membership and rebalances to viewers.
- Synchronous socket writes from the listener could stall consumption behind a slow client.
- Unbounded queues or durable browser delivery would add memory growth or persistence semantics beyond the initial teaching requirement.

## Consequences

Browser clients observe copies rather than participating in Kafka consumer-group assignment. WebSocket delivery is best-effort and may be lost even after Kafka offsets are committed. Discarding a display event does not delete its Kafka record.

Reconnect resumes new observations; it does not replay missed browser events. Kafka restarts can still replay uncommitted records or process backlog. Multiple backend instances in the same group would divide partitions, so their local viewers would receive different subsets. Scaling that model requires a new decision.
