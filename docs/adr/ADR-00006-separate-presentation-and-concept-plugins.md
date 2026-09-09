# ADR-00006: Separate presentation navigation and concept plugins

Date: 2026-09-07

Status: Accepted

## Context

The owner wants a browser presentation with directed navigation and pluggable live Kafka illustrations. A new concept should reuse the surrounding scaffolding. The first implementation exercises this separation with a partitioning lesson.

## Decision

Separate the presentation shell, application-level live connection handling, shared visual components, and individual concept plugins. Let a slide reference a plugin with configuration, so the same illustration can support several lessons.

Start with explicitly registered, compiled-in plugins. A concept would supply its configuration, event-to-state handling, rendering, and optional presenter controls. Validate the boundary with a partitioning lesson and then a second concept before formalizing a broad extension API.

Keep observations grounded in actual Kafka metadata. For example, partition buckets use consumed partition IDs; fading old cards changes only the display. Navigation should not create Kafka consumers or cause rebalances.

## Alternatives

- A self-contained implementation for every slide would be initially direct but duplicate streaming and illustration code.
- Runtime-loaded third-party plugins would add packaging and compatibility concerns before they are needed.
- A fixed simulation could explain concepts but would not meet the central goal of observing the impact of changes to real code and Kafka.

## Consequences

New concepts could share connection handling and visual primitives while owning their teaching behavior. Some lessons would still need new backend instrumentation; consumption events alone cannot explain every Kafka mechanism.

Use reveal.js 5.2.1 and plain JavaScript modules served directly by Spring Boot. This keeps one origin for assets, HTTP and WebSocket traffic. Pin reveal.js through npm and check in the runtime assets and license so JVM builds and presentations do not require Node or CDN access. React/TypeScript can be revisited if the illustrations become complex enough to justify them.

The initial registration maps a `data-concept` name to a mount function returning `onRecord` and `reset`. The deck mounts concepts once; one live client forwards observations independently of navigation. Treat this concrete API as provisional until a second concept validates it. Partition columns appear only when observed because the existing API does not expose partition inventory.
