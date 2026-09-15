# Working on Kafka demo core

Start with INTENT.md and CONTEXT.md; use README.md for build commands. This checkout
contains shared libraries, not the original slideset. Lessons and experiments live
in `../kafka-lessons`; migration views and integrations in `../kafka-migration-demo`.

For slides or live concepts, read docs/ADDING_LESSONS.md and use the project-local
`.agents/skills/add-kafka-lesson/SKILL.md`, then read the target demo's instructions.
A concept plugin is an in-app module, not a Codex plugin bundle. Keep one-off features
with their demo and shared dependencies pointing from demos to core only.

Preserve predict → run → observe → inspect code → change one thing. Navigation must
not start consumers or reconnect streams. Read observation limits before proposing
batching, commits, replay or replication claims. Maintain the authoring contract when
it changes. Keep machine-specific permission workarounds out of application config.
Do not assume historical external broker or runtime observations remain current.
