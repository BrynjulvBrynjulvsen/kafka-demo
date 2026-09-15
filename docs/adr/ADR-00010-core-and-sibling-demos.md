# ADR-00010: Shared Kafka libraries and explicit sibling demo applications

Date: 2026-09-12
Status: Accepted and implemented.

## Context

The combined application bundled lessons, experiment workers and migration/Kubernetes
observations in one JAR. Profiles suppressed unrelated services but did not remove
their code, assets or configuration. The owner requested kafka-demo as core,
kafka-lessons for the original lessons, and kafka-migration-demo for migration.

## Decision

Keep backend and presentation library modules in kafka-demo. Both demos are independent
Spring Boot applications declaring normal core artifact dependencies. An explicit
`kafkaDemoCore` property enables Gradle composite substitution for sibling development.
Core has no dependency on either demo and no executable application or default deck.

Kafka production, observation and named connection settings are core capabilities,
activated through explicit configuration imports. Transport-only use requires no topics.
Applications own environment defaults and runtime lifetimes. Experiment loops stay
readable in lessons; Kubernetes, telemetry schemas and migration views stay in migration.

Package shared Reveal assets, palette/layout primitives and browser plumbing in a
resource JAR under `/kafka-demo/`. Each demo owns its HTML and feature adapter. Core
transport delivers envelopes without interpreting experiment or migration payloads.
Features own evidence, validation and freshness semantics. Views register explicitly;
there is no dynamic plugin discovery, installation system or generic worker engine.

Preserve existing HTTP/WebSocket paths and version-1 envelopes. Keep consumed records
transient and snapshots retained. Navigation never starts consumers, changes backend
membership or reconnects the stream. One-off features stay demo-local until reused.

## Alternatives

- More Spring profiles: insufficient dependency and asset isolation.
- Copy core into each demo: creates diverging transport and presentation behavior.
- Mandatory published artifacts now: unnecessary for sibling development; use
  explicit composite builds until independent releases justify publication.
- A generic non-Kafka platform: all intended demos use Kafka, so client capabilities
  belong in the base backend while Kubernetes and special experiments remain optional.

## Consequences

Each demo builds and tests separately, and core changes must be checked against both
consumers. Separate composite invocations must not concurrently write the same core
build outputs. A new demo needs explicit dependencies, application wiring, its own
deck and configuration. Core artifacts include no credentials or environment defaults.
The static-only presentation library can be consumed separately from backend support.

This supersedes ADR-00006/00009's combined-application placement and ADR-00007's
placement of the experiment runtime in core. Their observation and lifecycle
semantics remain valid. Historical ADRs retain their original implementation context.

## Runtime deployment

Each demo packages its executable JAR into its own Docker image. Local credentials
remain read-only mounts from that demo's ignored `.local` directory. The optional
POC Compose setup uses a separate legacy-port-forward service per demo because the
broker advertises localhost:32095. The application shares that service's network
namespace. The lessons application itself has no Kubernetes client; migration
includes kubectl for read-only observation. Runtime processes have no mounts or
artifact dependency on the core checkout. The combined runtime was retired after
successful live verification on 2026-09-12.
