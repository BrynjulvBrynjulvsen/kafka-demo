# Kafka demo core

Shared Kafka backend and presentation libraries for explicit sibling demo applications.

**Running or editing the lesson slides?** Follow the participant
[setup guide](../kafka-lessons/SETUP.md). Open `kafka-lessons` in your IDE and keep
this core checkout beside it; this repository is a library, not the slide application.

| Project | Owns |
| --- | --- |
| `kafka-demo` | Kafka capabilities, observation transport, theme and presentation primitives |
| [kafka-lessons](../kafka-lessons/README.md) | Original slides, producer controls and consumer experiments |
| [kafka-migration-demo](../kafka-migration-demo/README.md) | Migration slides, Kubernetes observations and legacy telemetry |

## Build

JDK 17 and the included Gradle wrapper:

```sh
./gradlew test assemble
```

The backend tests start embedded Kafka; no workshop, Kubernetes, or sibling demo
checkout is required. This project produces ordinary `backend` and `presentation`
JARs, not an executable application. Each demo builds its own Boot JAR.

Docker alternative:

```sh
docker run --rm -v "$PWD:/workspace" -v kafka-demo-gradle-cache:/home/gradle/.gradle \
  -w /workspace gradle:8.14.3-jdk17 gradle --no-daemon test assemble
```

## Develop a demo with local core changes

From either demo's root:

```sh
./gradlew -PkafkaDemoCore=../kafka-demo test bootJar
./gradlew -PkafkaDemoCore=../kafka-demo bootRun
```

Each demo declares normal module dependencies. The explicit property enables a Gradle
composite build that substitutes local core projects. Without it, Gradle resolves
published coordinates through the configured repositories. No artifacts have been
published by this restructuring, so use the local property for now. An explicitly
supplied missing path fails clearly. Do not run simultaneous Gradle invocations that
write the same included build outputs; sequence consumer builds or use separate CI
checkouts.

## Backend capabilities

Import only the configurations required by your application:

- `KafkaConnectionsConfiguration`: optional named standard Kafka settings and local
  client-properties loading; creates no network clients.
- `KafkaProducerConfiguration`: bounded string production and topic discovery.
- `KafkaObserverConfiguration`: selected-topic observation and its WebSocket route;
  starts the backend-owned observer independent of browser viewers.
- `ObservationTransportConfiguration`: bounded WebSocket delivery for explicitly
  registered `ObservationRoute` beans, with no topic/producer requirements.

Core Kotlin package: `io.bekk.kafkademo.core`. Do not component-scan this package;
explicit imports are the capability boundary. There is no core `application.yml`.
Applications own bootstrap servers, groups, topics, origins, ports and active services.
Spring Kafka's standard client factories are available; specialized teaching loops
remain in their owning demo. See [authoring](docs/ADDING_LESSONS.md) for examples.

## Presentation library

Assets are packaged under `META-INF/resources/kafka-demo/` and served by the consuming
Spring application under `/kafka-demo/`. The library has no entry HTML page.

- `css/theme.css`: palette and shared layout primitives.
- `js/deck.js`: Reveal initialization, explicit concept mounting and isolated dispatch.
- `js/live-client.js`: one reconnecting observation connection.
- `js/kafka-client.js`: record validation, topic discovery and explicit production.
- `vendor/reveal/`: locally bundled Reveal assets and license.

Experiments and migration payloads are interpreted by demo-local adapters. To update
vendored Reveal assets intentionally, run `npm ci && npm run vendor` here. Normal
JVM builds do not need Node or an asset download.

## Verification across projects

Run `scripts/check-demos.sh` from a workspace containing both sibling demos. It runs
core and consumer backend tests/builds sequentially and checks packaged ownership.
Browser suites live in the demos; lessons supplies an isolated embedded-Kafka browser
server, and migration supports fixture rendering with observation disabled. Their
READMEs distinguish fixture checks from live external infrastructure verification.

See [INTENT](INTENT.md), [CONTEXT](CONTEXT.md), [architecture decisions](docs/adr/README.md),
and the [restructuring plan](docs/RESTRUCTURING_PLAN.md).

## Running the demos

The standalone demo images include their own JARs and shared assets. Core is required
only to build them. Each demo's README documents its optional POC Compose setup and
local credentials. A core `clean` does not affect either running application; there
are no runtime mounts from this checkout. The old combined runtime and temporary
restructuring backups have been removed.
