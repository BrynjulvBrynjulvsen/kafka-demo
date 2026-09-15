# Project intent

Maintain a small shared foundation for Kafka demonstrations. Kafka client support,
bounded browser observation delivery and presentation primitives belong here. Each
explicit demo owns its teaching content, experiments, specialized integrations and
operational settings in a sibling project.

Kafka capability availability is distinct from activation. A demo selects observers,
producer APIs and channels explicitly. Browser navigation must never change backend
consumer membership or reconnect streams. Preserve the distinction between record
consumption, processing, commits, replication and browser rendering.

Keep the teaching loop readable: predict, run, observe, inspect code, change one thing.
One-off features stay with their demo; extract shared code only when it is reused.
The core is a library, not a default combined demo or general Kafka administration UI.
