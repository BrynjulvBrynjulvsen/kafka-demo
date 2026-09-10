# Project intent

Last updated: 2026-09-09

## Goal

Build a set of utilities for demonstrating and teaching Apache Kafka. Attendees should be able to see Kafka behavior illustrated live, inspect the actual code responsible for it, and observe the impact when the presenter changes that code or its configuration.

Favor small, readable examples and reusable scaffolding. The teaching loop is: predict, run, observe, inspect the code, change one thing, and run again.

## Intended experience

- A browser presentation supports directed navigation through lessons. A slide framework such as slides.js, or ideas from one, can provide the navigation.
- Slides host pluggable live concept illustrations. Adding a concept should reuse the presentation, connection handling, and visual building blocks wherever practical.
- Illustrations are simple and grounded in actual Kafka observations. For partitioning, messages arrive in partition buckets, the most recent few remain visible, and older ones fade out. Fading is a display operation, not Kafka record deletion.
- A Kotlin backend connects to a configurable Kafka cluster and streams consumed observations to browsers. It continues consuming when nobody is watching, logging and discarding those observations from the browser delivery pipeline.
- The eventual self-contained setup includes an optional Docker Compose KRaft cluster with two brokers. Its controller topology remains to be decided.
- The code remains part of the lesson: presenters can find and tweak producers, consumers, and relevant settings without first understanding a large framework.

## Current scope

The first increment is a minimal backend. It produces messages to configured topics over HTTP and lets WebSocket clients select a configured topic to observe. It uses Spring Boot and Spring Kafka, following the initial preference for those libraries.

For now, reuse the already-running broker from the sibling `kafka-workshop` repository. That repository is reference material and supplies the current infrastructure. Work primarily in `kafka-demo`. The workshop's separate second cluster was removed by the owner; adding a second broker to a cluster is future work.

The backend and first reveal.js presentation are implemented. The partitioning lesson uses HTTP production and WebSocket observations, with a provisional concept boundary for future illustrations. Runtime presentation assets are bundled locally, and live browser tests exercise the real broker. A Compose setup owned by this repository is not implemented yet.

## Boundaries

This is a local teaching application. Production hosting, authentication, arbitrary runtime plugin loading, durable browser event delivery, and a general-purpose Kafka administration UI are outside the first increment.

Do not imply guarantees the observations cannot establish. A consumed record does not by itself prove application processing completion, offset commit, or replication state. Browser reconnection and Kafka replay are different operations.

## Evolution

- 2026-09-09: Selected and implemented the next lesson sequence: ordering, consumer groups, offsets/replay, and lag. Dedicated opt-in experiment consumers supply the missing observations while independent visual modules reuse the shared transport. BACKLOG.md tracks the remaining concepts.

- 2026-09-08: Prioritized a documented slide/concept authoring workflow and reusable agent skill. Identified batching and null-key sticky partitioning as a future standalone lesson requiring care about what producer behavior is actually observable.

- 2026-09-07: Established the browser presentation, pluggable live illustrations, Kotlin streaming backend, and optional two-broker KRaft setup as the project direction.
- 2026-09-07: Narrowed implementation to the backend first, using the workshop's existing broker and adding HTTP production and per-topic WebSocket subscriptions.

Update this document when the owner's goals or scope change. Record architectural choices and their tradeoffs in [ADRs](docs/adr/README.md); keep the current implementation and working context in [CONTEXT.md](CONTEXT.md).
