# ADR-00002: Use HTTP commands and WebSocket events

Date: 2026-09-07

Status: Accepted

## Context

Presenters need to send messages, and browser illustrations need a continuous stream of observations. These interfaces should also be usable without a frontend so the backend can be developed and taught independently.

## Decision

Expose message production through `POST /api/messages` and configured topic discovery through `GET /api/topics`. Return actual broker acknowledgment metadata for a successful produce request.

Expose plain JSON WebSockets at `/ws/topics/{topic}`. Send a `subscribed` event followed by versioned `record-consumed` events containing Kafka topic, partition, offset, timestamp, key, value, and headers. The initial event confirms viewer registration, not consumer readiness.

Use HTTP for commands and WebSocket for observations. Do not add STOMP, SockJS, or a second messaging broker. Default HTTP binding to loopback and validate browser WebSocket origins; authentication is outside the local first increment.

## Alternatives

- Server-sent events could serve the current one-way observation stream, but WebSocket matches the requested subscription interface.
- STOMP would add destinations and protocol conventions that the initial per-topic endpoint does not need.
- Commands over the same WebSocket would require request correlation and an additional command protocol.

## Consequences

HTTP requests can be demonstrated with curl and events with a browser's native WebSocket API. Clients must handle disconnects and versioned events. A successful HTTP response proves producer acknowledgment, not browser delivery. A failed or timed-out HTTP request does not prove the record was absent from Kafka, so HTTP retries may duplicate production.
