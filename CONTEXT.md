# Project context

Last updated: 2026-09-08

## Start here

- [INTENT.md](INTENT.md): why the project exists and its evolving scope.
- [README.md](README.md): runnable commands, endpoints, configuration, and operating behavior.
- [Architecture decisions](docs/adr/README.md): decisions, alternatives, and consequences.

## Current implementation

One Kotlin / Spring Boot application runs an HTTP API and plain JSON WebSocket endpoint. A Spring Kafka listener subscribes to the configured topic list at startup and fans consumed records out to viewers of each topic.

| Interface | Behavior |
| --- | --- |
| `POST /api/messages` | Produce a string value with an optional key and configured topic; return broker acknowledgment metadata |
| `GET /api/topics` | List the topic allowlist and default producer topic; not a broker health check |
| `/ws/topics/{topic}` | Subscribe a WebSocket viewer to an allowed topic; send a subscription acknowledgment followed by consumed records |

Topic selection is limited to startup configuration. There is no API for adding Kafka subscriptions or creating topics at runtime. Topics must exist before startup. Changing viewers does not change Kafka group membership.

The stream is best-effort. Records consumed without viewers are logged and discarded from the delivery pipeline while batch commits continue. There is no browser replay buffer or browser acknowledgment. Consumers can still redeliver records after a restart; `auto-offset-reset=latest` applies only when there is no valid committed offset. See [ADR-00003](docs/adr/ADR-00003-decouple-consumption-from-viewers.md).

## Code map

All application classes live under `src/main/kotlin/io/bekk/kafkademo`.

| File | Responsibility |
| --- | --- |
| [Application.kt](src/main/kotlin/io/bekk/kafkademo/Application.kt) | Application entry point and configuration-property scanning |
| [DemoProperties.kt](src/main/kotlin/io/bekk/kafkademo/DemoProperties.kt) | Validate configured topics and the producer default |
| [MessageController.kt](src/main/kotlin/io/bekk/kafkademo/MessageController.kt) | HTTP production and topic discovery |
| [TopicConsumer.kt](src/main/kotlin/io/bekk/kafkademo/TopicConsumer.kt) | Kafka listener and versioned observation model |
| [WebSocketConfiguration.kt](src/main/kotlin/io/bekk/kafkademo/WebSocketConfiguration.kt) | WebSocket routing, origin allowlist, and topic validation |
| [TopicWebSocketHandler.kt](src/main/kotlin/io/bekk/kafkademo/TopicWebSocketHandler.kt) | Per-viewer bounded queues and sender lifecycle |
| [application.yml](src/main/resources/application.yml) | Cluster connection, group, acknowledgment policy, and environment overrides |

The build currently uses Kotlin 2.3.21, Spring Boot 4.1.1, Gradle wrapper 8.14.3, and a JDK 17 toolchain. [build.gradle.kts](build.gradle.kts) is authoritative for dependency versions.

## Workshop infrastructure

The sibling `../kafka-workshop` repository provides reference exercises and the current Docker Compose infrastructure. Do not assume its old second-cluster configuration still exists or represents the intended future two-broker topology.

- Host bootstrap address: `localhost:9094`.
- Container bootstrap address on the `kafkaworkshop` network: `kafka1:9092`.
- Default topic and consumer group: `kafka-demo`.
- The demo topic was created with three partitions and replication factor one.
- The owner identified the current Colima Docker context as disposable and authorized its use for this work. Check the active context before assuming a later session has the same environment.

There is no repository-owned Compose file yet. See [ADR-00005](docs/adr/ADR-00005-reuse-workshop-kafka.md).

## Verification and last observed runtime

On 2026-09-07, `test bootJar` passed inside `gradle:8.14.3-jdk17`:

- Four integration tests used an embedded KRaft broker and real HTTP/WebSocket server to check fan-out, topic isolation, no-viewer commits and reconnect behavior, and request validation.
- One test checked that a blocked viewer does not block delivery to a healthy viewer and is disconnected on queue overflow.
- [SmokeTest.java](scripts/SmokeTest.java) verified HTTP production through the actual workshop broker to two WebSocket viewers with matching partition and offset metadata.
- A subsequent live record with no viewers was logged, and the consumer group committed its offset with zero lag.

At the end of that verification, the `kafka-demo` application container was left running with port 8080 published on host loopback. This is a dated observation, not a guarantee that it is still running. Check Docker state before starting another instance on the same port or in the same consumer group.

Host-side Gradle could read Java and dependencies but could not bind its daemon port under the task's execution permissions, even after escalation. Building and testing inside the authorized Docker context worked. The Docker route in the README is a development option, not a requirement of the application. Do not copy machine-specific task permission configuration into the project.

## Next work and open choices

The presentation now uses the owner's Bekk technology/design/product leadership palette: light Vann canvas, Dag panels, Jord supporting surfaces, Natt typography, Datablå accents and a Rebell producer button. Named palette tokens live in `css/slides.css`; the authoring guide records their exact values and excludes management consulting colors. Older cards fade through surface colors while retaining readable text.

The authoring workflow is documented in [docs/ADDING_LESSONS.md](docs/ADDING_LESSONS.md), with an ordinary slide recipe, a complete observation-counter concept recipe, the current shared-topic/singleton-control limitations, and verification guidance. [AGENTS.md](AGENTS.md) directs future repository agents to that guide and the project-local skill at `.agents/skills/add-kafka-lesson`. The skill passed the skill-creator validator; the recipe was exercised for independent instances, counting, and repeated reset. The owner requested batching and sticky partitioning as a future standalone lesson; its proposed experiment and missing instrumentation are recorded in the guide, not implemented as a new slide.

A reveal.js partitioning deck is implemented in `src/main/resources/static` and included in the Spring Boot JAR. It has a producer form, application-level reconnecting WebSocket, observed partition buckets with bounded cards, and prediction/code slides. reveal.js 5.2.1 and its license are bundled locally through the npm lockfile and `scripts/vendor-slides.js`; no CDN or external fonts are requested at runtime. JavaScript syntax checks and Docker `test bootJar` pass. Two Playwright browser smoke tests passed against the workshop broker, checking acknowledgments against consumed cards, same-key partitioning, four-card retention, literal HTML-like content, navigation retaining one socket, reconnect, and HTTP failure recovery. Screenshot review also caught and corrected a reveal.js background override.

The application container was rebuilt and restarted with the deck available at `http://localhost:8080/`. Host Chromium could not load macOS appearance files under task permissions; browser tests ran in `mcr.microsoft.com/playwright:v1.63.0-noble` with `--network container:kafka-demo` and the repository mounted at `/workspace`. Browser screenshots are in `build/slides-title.png` and `build/slides-live.png`. The normal local Playwright commands are documented in README.

The first suggested frontend lesson is partitioning. The presentation should show actual partitions and offsets while keeping navigation independent of the Kafka consumer lifecycle.

The first deck uses reveal.js and plain JavaScript modules. The provisional concept boundary exposes `onRecord` and `reset` from a registered mount function. Validate it with a second concept before formalizing it. See [ADR-00006](docs/adr/ADR-00006-separate-presentation-and-concept-plugins.md).

Later lessons may add consumer groups, processing and commits, replay, or replication and failure. Those require the relevant observations and controls; the current `record-consumed` event is not evidence for all of them. A second broker and the controller topology should be designed when the local cluster work begins.

## Maintaining these documents

Keep goals in INTENT, current facts and handoff notes here, and operating instructions in README. Add numbered ADRs for meaningful architectural decisions. Mark proposed choices explicitly; when a decision changes, add a superseding ADR and link it from the old one rather than silently replacing the history.
