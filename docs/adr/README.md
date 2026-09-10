# Architecture decision records

Use `ADR-0000X-short-decision-desc.md` filenames, continuing the five-digit sequence as the number grows. Each record includes its date, status, context, decision, alternatives, and consequences. Status distinguishes accepted decisions from proposals that have not been adopted.

| Record | Status | Decision |
| --- | --- | --- |
| [ADR-00001](ADR-00001-use-kotlin-and-spring.md) | Accepted | Kotlin, Spring Boot, and Spring Kafka for the backend |
| [ADR-00002](ADR-00002-use-http-commands-and-websocket-events.md) | Accepted | HTTP commands and plain JSON WebSocket observations |
| [ADR-00003](ADR-00003-decouple-consumption-from-viewers.md) | Accepted | Continuous consumption and bounded per-viewer delivery |
| [ADR-00004](ADR-00004-use-configured-topics-and-string-records.md) | Accepted | Configured topic allowlist and string records for the first increment |
| [ADR-00005](ADR-00005-reuse-workshop-kafka.md) | Accepted | Reuse the workshop broker and defer repository-owned cluster infrastructure |
| [ADR-00006](ADR-00006-separate-presentation-and-concept-plugins.md) | Accepted | reveal.js with separate navigation, live connection handling, and concept modules |

| [ADR-00007](ADR-00007-controlled-experiment-runtime.md) | Accepted | Explicit experiment workers, bounded snapshots and optional concept handlers |

These records document the direction in [INTENT.md](../../INTENT.md). See [CONTEXT.md](../../CONTEXT.md) for implementation status. If an accepted decision changes, create a new record and update the prior record's status and superseding link.
