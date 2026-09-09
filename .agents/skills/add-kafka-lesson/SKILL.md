---
name: add-kafka-lesson
description: Add or extend reveal.js slides and live Kafka concept modules in the kafka-demo project. Use for new Kafka lessons, illustrations, and presenter controls; not for creating Codex plugin bundles or general Kafka administration.
---

# Add a Kafka lesson

Work in the user's kafka-demo checkout. Confirm it contains `src/main/resources/static/js/slides.js` and `docs/ADDING_LESSONS.md`; if invoked elsewhere, locate the intended checkout from session context before editing. Do not hardcode a machine-specific checkout path.

Read `AGENTS.md`, `INTENT.md`, and `docs/ADDING_LESSONS.md` in that checkout. The authoring guide is authoritative for the current extension contract and contains a complete minimal concept recipe. Read `CONTEXT.md` and the root `README.md` when running the application or tests; runtime notes are dated observations.

Choose the smallest change that teaches the requested behavior:

- A prose/prediction/code slide usually needs only `index.html` and perhaps scoped CSS.
- A new view of consumed records uses a registered concept module with local DOM/state and the shared stream.
- A lesson about events not exposed by the backend requires explicit instrumentation or clearly labeled explanation; do not infer batch boundaries, commits, or replication from consumed cards.

Follow the existing prediction → action → observation → code-change teaching loop where it suits the request. Inspect the current registry, transport, and nearest relevant concept before implementing. Reuse the application-level connection; slide navigation must not create sockets or Kafka consumers. Account for the singleton producer form and shared selected topic described in the guide before adding another interactive lab.

Keep runtime assets local. Refresh vendor files only for a dependency change. Keep Kafka values as text, retained display history bounded, and production explicitly user-triggered with no automatic retry. Preserve distinctions between null and empty keys, broker acknowledgments and consumption, and browser reset/reconnect and Kafka replay.

Use the guide's validation section proportionally: render and inspect changed slides, test new live behavior against the local demo when applicable, and report mocked checks separately. Rebuilding a JAR requires restarting it to serve new assets. Inspect the active runtime before starting another consumer. Do not invent passing verification if the environment prevents a check.

Update the maintained guide if you change its contract. Keep implementation handoff in CONTEXT, operational changes in README, and architecture decisions in ADRs. Record future lesson ideas without implementing them unless requested. Finish with the entry URL or relevant source links, what the lesson demonstrates, validation, and any remaining evidence limits.
