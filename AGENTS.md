# Working on Kafka demos

Start with `INTENT.md` for goals and `CONTEXT.md` for implementation and runtime notes. Use `README.md` for commands.

When adding or changing reveal.js lessons or live concept plugins, read `docs/ADDING_LESSONS.md` and use the repository's `.agents/skills/add-kafka-lesson/SKILL.md`. A concept plugin here is an in-app JavaScript module, not a Codex plugin bundle.

Keep the teaching loop grounded in actual observations: predict, run, observe, inspect code, change one thing. Browser navigation must not create Kafka consumers or reconnect the deck's stream. Read the authoring guide's observation limits before proposing lessons on batching, commits, replay, or replication.

Maintain the authoring guide when the extension contract changes. Keep machine-specific permission workarounds out of application configuration. The sibling workshop supplies infrastructure; do not assume an old runtime observation is still current.
