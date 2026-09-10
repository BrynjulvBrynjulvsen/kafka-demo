# Project context

Last updated: 2026-09-09

## Start here

- [INTENT.md](INTENT.md): why the project exists and its evolving scope.
- [README.md](README.md): runnable commands, endpoints, configuration, and operating behavior.
- [Architecture decisions](docs/adr/README.md): decisions, alternatives, and consequences.

## Lessons added 2026-09-09

BACKLOG rows 1–4 now have prediction, experiment and inspect/change slides at
`/#/ordering`, `/#/groups`, `/#/offsets`, and `/#/lag`. Ordering uses the original
record stream. Groups, offsets and lag use an `ExperimentRuntime` with two
allowlisted groups, up to four members each, and the dedicated `kafka-demo-lab`
topic. `ExperimentController` provides bounded explicit commands. The worker loop
is a plain KafkaConsumer poll/process/commit loop separate from the Spring observer.
See README for environment flags, setup, restart/reset semantics and workload limits.

Concepts remain explicitly registered independent modules with onRecord/reset and
optional onExperiment(snapshot). Presenter controls live under `js/controls`.
Experiment snapshots share the single application socket and restore current state
on reconnect; navigation never starts workers. Backend history is capped at 48
recent events, ordering at six cards per lane, and lag at 40 samples. Staleness and
missing commits are explicit. ADR-00007 records this extension to ADR-00006.

The older implementation and runtime observations below remain historical context;
this section and README describe the extended deck.

## Current implementation

Experiments are enabled by default, with `kafka-demo` and `kafka-demo-lab` in the
initial topic allowlist. Create both topics before startup. Explicitly set
`DEMO_EXPERIMENT_ENABLED=false` to disable experiment sampling and commands; also
set `KAFKA_TOPICS=kafka-demo` when running without a lab topic. Enabled experiments
start broker sampling; worker membership and workloads still require commands.
See [experiment setup and use cases](README.md#ordering-groups-replay-and-lag-lessons).

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
See [Backend structure](README.md#backend-structure-and-code-to-teach-from) for a
responsibility diagram and the distinction between observation and experiment consumers.

| File | Responsibility |
| --- | --- |
| [Application.kt](src/main/kotlin/io/bekk/kafkademo/Application.kt) | Application entry point and configuration-property scanning |
| [DemoProperties.kt](src/main/kotlin/io/bekk/kafkademo/DemoProperties.kt) | Validate configured topics and the producer default |
| [MessageController.kt](src/main/kotlin/io/bekk/kafkademo/MessageController.kt) | HTTP production and topic discovery |
| [TopicConsumer.kt](src/main/kotlin/io/bekk/kafkademo/TopicConsumer.kt) | Kafka listener and versioned observation model |
| [ExperimentController.kt](src/main/kotlin/io/bekk/kafkademo/ExperimentController.kt) | HTTP experiment state, commands, and error responses |
| [ExperimentRuntime.kt](src/main/kotlin/io/bekk/kafkademo/ExperimentRuntime.kt) | Experiment workers, poll/process/commit loop, workloads, broker sampling, and offset resets |
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

The first deck uses reveal.js and plain JavaScript modules. The concept boundary exposes `onRecord` and `reset` from a registered mount function, with optional `onExperiment` for the group/progress modules. It is now exercised by multiple concepts; ADR-00007 documents the extension. See [ADR-00006](docs/adr/ADR-00006-separate-presentation-and-concept-plugins.md).

Consumer groups, commits/replay and lag now use the separate experiment observations described above. Replication and failure remain future lessons requiring their own evidence; `record-consumed` alone cannot establish those behaviors. A second broker and the controller topology should be designed when the local cluster work begins.

## Maintaining these documents

Keep goals in INTENT, current facts and handoff notes here, and operating instructions in README. Add numbered ADRs for meaningful architectural decisions. Mark proposed choices explicitly; when a decision changes, add a superseding ADR and link it from the old one rather than silently replacing the history.

## Verification on 2026-09-09

- Docker `test bootJar` passed all six backend tests, including the new embedded
  KRaft scenario for independent groups, four-member ownership, inactive-group
  replay, resume from commits, repeated stops and lag recovery.
- All four browser tests passed against the refreshed application: live ordering,
  group controls, topic selection, navigation/reconnect preserving member identity,
  bounded cards and safe text; mocked cases cover command failure recovery and
  stale/unknown offset displays. Interactive layouts were captured at 1440×960 and
  1024×768; explanation/code slides were also checked for viewport overflow.
- The current Docker context was checked as `colima-kafka-workshop`. Created the
  three-partition RF1 `kafka-demo-lab` topic and restarted `kafka-demo` with that
  topic allowlisted and experiments enabled. The previous container is retained,
  stopped, as `kafka-demo-before-lessons`. At verification end, workload production
  was stopped, all experiment members were stopped, and broker samples succeeded.
- Wait for actual observer assignment after restarting before running live tests.
  Do not run browser verification while replacing the JAR used by the running
  application; restart it after the build so its classloader sees a consistent JAR.

## Shared membership controls

The groups, offsets and lag panels now reuse `controls/group-membership.js`: A/B
selection, observed member count/status, partition chips, Add member and Stop group.
Initial offset policy is under Start position. Group status is derived from worker
snapshots and assignment coverage; stale state is Unknown, not an asserted broker
state. No backend or transport contract change was needed.

Widget verification: bootJar passed; two targeted browser tests passed (mocked
membership transitions/commands and command-error/stale-state behavior). Inspected
live assignments on all three panels at 1440×960 and 1024×768. Restored the user's
pre-refresh experiment configuration: one A member, two B members, 1000 ms delay.

Processing delay is now per group (`groupDelays` in snapshots), controlled in the
shared membership widget on groups/offsets/lag. Commands must identify the group;
new workers inherit that group’s current setting. The cap remains 1000 ms/record.


## Architecture documentation review, 2026-09-10

ADR-00007 records the independent worker runtime and shared snapshot transport.
ADR-00008 now records the shared widget boundary, panel-local selection versus
backend-owned settings, per-group delay lifetime, and limits of inferred status.
The experiment payload changed from scalar delayMs to groupDelays without an
experiment envelope version bump; bundled frontend/backend must be refreshed
together. The original consumed-record protocol is unaffected.
