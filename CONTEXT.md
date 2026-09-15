# Current implementation

Updated 2026-09-12. See INTENT.md for goals, README.md for commands and ADR-00010
for the accepted ownership boundaries.

## Three-project split

`kafka-demo` is now a Gradle build with `backend` and `presentation` library modules.
It has no runnable application, default deck or main application YAML. Backend
integration tests have their own test-only application and embedded Kafka.

The original lessons and experiments are in `../kafka-lessons`, an independent
Spring Boot application with explicit core dependencies. It includes an embedded
Kafka `browserTestServer` task for reproducible browser checks without external
infrastructure. Topic observation and production are explicitly imported core
capabilities. Lesson adapters own experiment validation, freshness and commands.

The migration application lives in `../kafka-migration-demo`, with its existing
Git metadata preserved. The three-directory split is complete. Future lesson plans,
including `docs/NEXT_DEMOS.md`, belong in `kafka-lessons`; migration backlogs and
runtime instructions belong in `kafka-migration-demo`.

## Standalone runtimes, verified 2026-09-12

The operational transition is complete. Each sibling has a Dockerfile, restricted
.dockerignore, `scripts/build-image.sh`, and optional `compose.poc.yml`. Application
JARs are copied into images, never bind-mounted. Runtime credentials belong to each
demo's ignored `.local` directory. The core is only a build-time dependency.

- `kafka-lessons-app-1` serves port 8080 using `kafka-lessons:local` and the POC
  legacy profile. Its optional `legacy-forward` service supplies localhost:32095;
  the lessons application contains no kubectl or Kubernetes configuration.
- `kafka-migration-demo-app-1` serves port 18080 using `kafka-migration-demo:local`.
  It has its own forwarding service and read-only Kubernetes observation settings.
- All old combined/demo-preview/test containers, the combined JAR, root `.local`,
  staging/baseline/source backups and the sibling migration source archive were
  removed after live verification and a Docker mount audit.
- No remaining Docker container mounts the core checkout. Both applications were
  restarted after cleanup; a full core Gradle clean succeeded. Both demos remained
  operational afterward, and no container mounts reference the core checkout.

Use the sibling READMEs for start/stop/rebuild commands. Neither runtime requires
retaining core build artifacts, a core checkout mount, or the other demo's process.
The external Kind POC was not provisioned or migrated during this transition.

## Shared contracts

- Core package: `io.bekk.kafkademo.core`; import capability configurations explicitly.
- No experiment/migration imports, payload dispatch, routes or assets in core main source.
- Shared assets are a resource JAR at `/kafka-demo/`, including local Reveal assets.
- Demo adapters own domain parsing and source freshness; core owns connection status.
- Logical routes are registered through `ObservationRoute`. Existing endpoint paths,
  version-1 envelopes, transient records and retained snapshots remain compatible.
- Each demo declares backend/presentation version `0.1.0-SNAPSHOT`; explicit
  `-PkafkaDemoCore=../kafka-demo` enables composite source substitution. No artifact
  publication is configured or required for local development.
- The authoring skill and guides now locate content in the appropriate sibling.

## Verification

The original combined application's baseline built successfully, with 12 of 13
backend tests passing. Its POC configuration test saw an embedded broker's leaked
JVM bootstrap property instead of the YAML default. The relocated test isolates
system properties from its configuration fixture. No production configuration was
changed to work around that issue.

Final JVM checks: core 6, lessons 2, migration 6 tests; all 14 pass. The migration
integration test now verifies isolation without any migration profile, including
absent lesson endpoints/assets and successful shared static asset delivery.
The original verification used a staged migration source mounted at the intended
sibling path. After promotion, `scripts/check-demos.sh` passed against the actual
sibling directories, including executable JAR ownership checks from `scripts/check-packages.py`.
Both executable artifacts contain their own application/deck plus shared libraries.

Browser verification uses Playwright 1.63.0 in Docker. Lessons runs against its own
real embedded Kafka broker; migration uses mocked observations with its runtime
disabled. The membership test now awaits command arrival and completion before
asserting the subsequent socket-driven transition, eliminating a fixture race.
Final browser results: lessons 5 passed; migration fixtures 4 passed, with 1 external
POC live test intentionally skipped. Desktop/laptop images
were inspected for lesson partitioning/lag and migration topology/flow; layout and
observation labels remain intact. External POC live tests are explicitly skipped.

Verification used the documented Docker route because host Gradle could not bind
its daemon socket in that session. Browser tests used the existing matching Playwright
installation after npm downloads failed. Package/lockfile declarations match; no
machine-specific workaround was added to application configuration.

## Live runtime verification

After switching the observers to their standalone images, the lessons browser suite
passed four checks (production/observation, ordering and group controls, reconnect,
and mocked error handling). The opt-in migration live browser test also passed with
fresh Kubernetes and telemetry, increasing received observations, navigation and
reconnect. The old migration runtime had been waiting for assignment; its replacement
successfully assigned and resumed consumption. No malformed telemetry was reported.

The previous lesson settings were restored after verification: one Group A worker,
zero processing delay, and no active workload. Observer groups were preserved and
old observers stopped before replacements started. In-memory migration counters and
histories reset on restart; Kafka observer commits were retained.
