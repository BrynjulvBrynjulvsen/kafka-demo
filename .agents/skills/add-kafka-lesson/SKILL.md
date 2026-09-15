---
name: add-kafka-lesson
description: Add or extend reveal.js slides and live Kafka concept modules in Kafka demo applications using the shared kafka-demo core. Use for new Kafka lessons, illustrations, and presenter controls; not for creating Codex plugin bundles or general Kafka administration.
---

# Add a Kafka lesson

Locate the intended demo from session context: original lessons live in the sibling
`kafka-lessons`, migration views in `kafka-migration-demo`, and reusable libraries in
`kafka-demo`. Do not assume a slideset lives in the core checkout or hardcode an
absolute machine path. Read the target's AGENTS.md, INTENT.md, CONTEXT.md and README.
Read the core's docs/ADDING_LESSONS.md for shared contracts and the target demo's
same-named guide for feature recipes. Prefer a demo-local feature; change core only
when the shared boundary actually needs to change.

Choose the smallest change that teaches the requested behavior:

- A prose/prediction/code slide usually needs only `index.html` and perhaps scoped CSS.
- A new view of consumed records uses a registered concept module with local DOM/state and the shared stream.
- A lesson about events not exposed by the backend requires explicit instrumentation or clearly labeled explanation; do not infer batch boundaries, commits, or replication from consumed cards.

Follow the existing prediction → action → observation → code-change teaching loop where it suits the request. Inspect the current registry, transport, and nearest relevant concept before implementing. Reuse the application-level connection; slide navigation must not create sockets or Kafka consumers. Account for the singleton producer form and shared selected topic described in the guide before adding another interactive lab.

Keep runtime assets local. Refresh vendor files only for a dependency change. Keep Kafka values as text, retained display history bounded, and production explicitly user-triggered with no automatic retry. Preserve distinctions between null and empty keys, broker acknowledgments and consumption, and browser reset/reconnect and Kafka replay.

Use the guide's validation section proportionally: render and inspect changed slides, test new live behavior against the local demo when applicable, and report mocked checks separately. Rebuilding a JAR requires restarting it to serve new assets. Inspect the active runtime before starting another consumer. Do not invent passing verification if the environment prevents a check.

Update the maintained guide if you change its contract. Keep implementation handoff in CONTEXT, operational changes in README, and architecture decisions in ADRs. Record future lesson ideas without implementing them unless requested. Finish with the entry URL or relevant source links, what the lesson demonstrates, validation, and any remaining evidence limits.
