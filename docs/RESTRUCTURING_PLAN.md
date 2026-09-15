# Kafka core and sibling demo projects

Status: Implemented in all three sibling projects, 2026-09-12. See CONTEXT.md for verification.

## Intended outcome

Use three sibling project roots:

```text
code/
  kafka-demo/             Shared Kafka backend and presentation libraries
  kafka-lessons/          Runnable original lessons and experiment runtime
  kafka-migration-demo/   Runnable migration presentation and observation runtime
```

Treat each as an independent build, suitable for its own repository. Preserve any
existing Git metadata; repository initialization and history transfer are separate
implementation steps. The core must build without either demo checkout. Each demo
depends on core, never on the other demo. Core contains no default lesson deck or
migration application. A test-only application exercises core integration behavior.

## Project contents

`kafka-demo` has two initial Gradle library modules:

- `backend`: Spring HTTP/WebSocket support, bounded observation delivery, Kafka
  connection/authentication settings, basic producer and observer capabilities,
  record envelopes, and small client-construction helpers where code needs them.
- `presentation`: shared Reveal assets/license, base theme, deck initialization,
  concept mounting/error isolation, and reusable browser transport. Package these
  as a resource JAR under a distinct `/kafka-demo/` asset namespace, served through
  the consuming Spring application's static resources.

Only demo applications produce executable Boot JARs. The core libraries produce
ordinary JARs. Node tooling remains for asset maintenance and browser verification;
normal JVM packaging uses checked-in vendor assets and needs no frontend bundler.

Each demo initially has one application module, its own Gradle wrapper/build,
package.json/lockfile for browser tests, configuration, documentation and scripts.
Do not create a Gradle module for every lesson, visualization or local feature.

| Existing files/responsibility | Destination and treatment |
| --- | --- |
| `Application.kt` | Replace with a small entry point in each demo; core gets a test application only |
| `DemoProperties.kt` | Core capability settings; separate transport/origin settings from topic/producer validation |
| `MessageController.kt`, `TopicConsumer.kt`, record DTOs | Core, explicitly enabled by the consuming application |
| `TopicWebSocketHandler.kt` | Core; separate transient event delivery from retained latest snapshots and remove experiment naming |
| `WebSocketConfiguration.kt` | Core routing infrastructure; demo/features register their channels and capability-specific routes |
| `ExperimentController.kt`, `ExperimentRuntime.kt` | `kafka-lessons`; preserve the readable poll/process/commit loop |
| `MigrationKubernetes.kt`, `MigrationRuntime.kt`, `MigrationState.kt` | `kafka-migration-demo`, including controller, properties, snapshot schema and channel registration |
| `index.html`, `slides.js`, original concept modules and experiment/ordering controls | `kafka-lessons` |
| `migration.html`, `migration.js`, migration concepts, `migration.css` | `kafka-migration-demo` |
| `live-client.js` | Split reusable connection and basic Kafka API support into core; experiment and migration adapters stay with their demos |
| `slides.css` | Extract palette/layout primitives into core; lesson-specific selectors stay with lessons |
| Reveal vendor assets and `vendor-slides.js` | Core presentation module, with updated paths |
| `application.yml` | Move environment/default topic/group choices into demos; core supplies no application-wide YAML |
| `application-poc-legacy.yml` | Lessons: it configures lessons against the POC, not the migration demo |
| `application-migration.yml` | Migration application defaults; remove flags used solely to suppress lesson components |
| `SmokeTest.java` | Lessons, retaining its explicit production behavior |
| `scripts/decks.sh` | Replace with per-demo run/status scripts; retire cross-demo container ownership from core after documenting replacements |

## Explicit composition

Offer small explicit Spring configuration imports for shared transport, basic Kafka
production and topic observation. Avoid broad component scanning of the core package
and profile expressions such as `!migration`. Migration imports transport and Kafka
connection support without creating the basic lesson observer or producer endpoint.

Kafka libraries are part of the base backend. Their presence does not start clients,
require lesson topics, sample brokers or expose commands. Validate a capability's
required settings when that capability is enabled. Allow named connection settings
so specialized consumers can target their intended cluster; retain existing demo
environment variables through adapters during the move. Do not build a universal
Kafka administration API or replace teaching loops with a generic worker engine.

The observation publisher accepts explicitly registered logical channels. Preserve
the current `/ws/topics/{topic}` and `/ws/migration` routes during migration. Keep
transient consumed events distinct from retained snapshots, and scope topic snapshots
to their topic. Preserve envelope versions and subscription acknowledgments while
removing domain-specific dispatch from the shared transport. Features own payload
validation, source freshness and semantic claims; core owns socket delivery/status.

Frontend extensions remain explicit imports and registrations. A demo wires its
concepts, controls and channel adapter once. Navigation never starts consumers,
changes subscriptions or reconnects the stream. Commands remain explicit, and
uncertain production acknowledgments are never automatically retried.

One-off extensions live in their demo's normal source directories. They can register
routes, publish observations and mount views through the same core interfaces. Move
them into a reusable library only when another demo needs them. Kubernetes and
migration telemetry schemas remain local to migration in this first restructuring.

## Build and dependency workflow

Give the two core artifacts stable coordinates, for example
`io.bekk.kafkademo:backend` and `io.bekk.kafkademo:presentation`, with a shared version.
Both demos declare normal dependencies on these coordinates.

For sibling development, use an explicit Gradle composite-build property, such as
`-PkafkaDemoCore=../kafka-demo`, and `includeBuild` dependency substitution. A demo
can also consume a published version without the core checkout. Document the local
composite command as the initial happy path; publication is a later release task,
not a prerequisite for the split. An explicitly supplied missing path fails clearly.
Do not silently switch dependency source based on whether a sibling folder exists.

CI for the initial split checks out core and each demo and uses composite builds.
Core changes run core checks and both consumer suites. Demo changes run that demo's
checks against the selected core revision. Record compatible revisions until released
artifact versions provide that boundary. No package registry is required initially.

Proposed commands from each demo root:

```sh
./gradlew -PkafkaDemoCore=../kafka-demo test bootJar
./gradlew -PkafkaDemoCore=../kafka-demo bootRun
```

These commands are implemented for both sibling demo projects.

## Implementation sequence

1. Establish the baseline: inspect working trees and existing runtimes, inventory
   source/assets/tests/local configuration, and record relevant test results. Do not
   overwrite sibling files or copy credentials, `.git`, build outputs or caches.
2. Add core library modules and narrow the transport/configuration interfaces while
   the existing application still exercises both demos. Preserve observable behavior.
3. Populate `kafka-lessons` with its application, assets, experiments, configuration
   and tests. Wire the composite dependencies; build and verify it independently.
4. Populate `kafka-migration-demo` with its application, assets, observation runtime
   and tests. Verify that it requires no workshop topics or experiment configuration.
5. After both applications pass, remove their old source/resources from `kafka-demo`
   and remove its runnable application and demo profiles. Inspect each packaged JAR
   for asset collisions and unwanted demo content.
6. Update run scripts, source links shown in slides, README/INTENT/CONTEXT, authoring
   instructions and skill paths. Add a superseding ADR and retain historical ADRs
   with links to the new ownership. Move lesson/migration backlogs to their demos.

Keep existing running applications until replacements are ready. Live verification
must stop/replace the relevant old observer before starting a replacement using the
same group. Do not provision infrastructure or run a migration as part of this move.

## Verification and completion criteria

- Core: retain `BackendIntegrationTest` using a test app and embedded Kafka, plus
  bounded WebSocket tests. Verify explicit capability activation and absence of
  unintended clients/routes when only transport is imported.
- Lessons: move `ExperimentIntegrationTest`, `PocLegacyConfigurationTest`, and
  `slides`, `lessons`, `membership` browser suites. Verify production, experiment
  commands, navigation/reconnect, bounded displays and error recovery.
- Migration: move `MigrationIntegrationTest`, `MigrationObservationTest`, and all
  migration browser suites. Run fixtures before separately identified live checks;
  retain independent Kubernetes/telemetry freshness and observation limits.
- Both runnable artifacts serve their own deck plus shared assets offline, retain
  current URLs initially, and contain none of the other demo's source/assets/routes.
- Lessons runs without kubectl/Kubernetes configuration. Migration runs without
  lesson topics, producer controller or experiment workers. Core tests need neither
  sibling project nor external infrastructure.
- Inspect both decks at presentation and laptop sizes after resource/CSS moves.
- A documented new-demo recipe requires only core dependencies, explicit backend
  capability wiring, a deck entry point and local configuration. Adding a local
  view or one-off feature does not require editing core.

## Documentation ownership

Core owns reusable extension contracts and a generic demo authoring skill. Each demo
owns its teaching goals, operational instructions, evidence limitations and local
feature recipes. Update the existing `add-kafka-lesson` skill so it no longer assumes
lesson assets live inside `kafka-demo`. Keep historical architectural records clearly
marked; this proposal does not retroactively change accepted implementation facts.
