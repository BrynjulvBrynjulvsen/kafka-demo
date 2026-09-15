# ADR-00009: Separate migration deck and read-only observation runtime

Date: 2026-09-10
Status: Accepted

Application placement superseded by [ADR-00010](ADR-00010-core-and-sibling-demos.md). Observation/lifecycle decisions remain applicable.


## Context

The sibling kafka-proxy-poc presents Kroxylicious and Cluster Linking across two
Kubernetes-hosted Kafka clusters. A single selected-topic record stream cannot
represent proxy selection, pod status, and client telemetry truthfully. The POC's
curated scripts already own setup and migration.

## Decision

Add `/migration.html` with explicitly registered topology, client and timeline
views. Reuse reveal.js, the palette, LiveClient reconnect handling and bounded
WebSocket delivery. `/ws/migration` is a logical snapshot channel, separate from
Kafka topic routes, with version-1 `migration-snapshot` envelopes. `GET
/api/migration` returns the same state. No migration mutation endpoint is added.

A startup-owned MigrationRuntime independently samples Kubernetes and consumes
telemetry directly from legacy with a dedicated kafka-demo-migration-* group.
It never runs setup, migration, scaling or port-forward commands. The migration
Spring profile disables the ordinary observer, experiment runtime and HTTP producer
controller, so it can run without the workshop infrastructure.

The initial Kubernetes adapter uses a fixed argument-vector kubectl invocation,
explicit context and namespace, request/process timeouts and bounded output. It
samples Pods, Deployments, Services, EndpointSlices and ConfigMaps every three
seconds after completion. Only public allowlisted configuration fields reach the
browser. No credentials or raw Kubernetes objects are streamed. This adapter is
replaceable with an SDK watch without changing the presentation contract.

Snapshots retain independent source timestamps/errors, 100 consumer member/topic
identities, 100 producer/topic identities and 48 recent events. Browser navigation
never changes observation lifetimes. Reconnect restores retained state; backend
restart resets the observation window. Telemetry broker responses, rather than
empty polls alone, establish source freshness. Observation counts can include
redelivery and multiple groups and are not integrity claims.

## Alternatives

- Add migration to the selected-topic experiment API: rejected because this state
  spans infrastructure and multiple data sources, rather than one experiment topic.
- Fork the whole application into the POC: unnecessary duplication of presentation
  and transport. POC instrumentation may still be extended separately.
- Kubernetes SDK/watch immediately: reasonable later, but kubectl uses the existing
  local authentication workflow without adding dependencies in this first increment.
- Parse human migration logs into phases: deferred in favor of structured events
  when script instrumentation is added.

## Consequences

Local runs need kubectl, explicit context read access and legacy Kafka connectivity.
The migration deck labels configured route separately from actual connections,
loaded configuration, traffic, replication status and producer progress. Missing
sources are unknown, not zero or healthy. The external Streams canary initially
has Kubernetes status only. Actual Cluster Linking observations and client
acknowledgment telemetry remain backlog work.
