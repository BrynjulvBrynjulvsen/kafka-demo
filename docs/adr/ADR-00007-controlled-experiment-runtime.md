# ADR-00007: Explicit experiment workers and shared observations

Status: Accepted, 2026-09-09

Lessons about groups, commits and lag need evidence beyond observer record cards.
Use a backend-owned, opt-in runtime on a dedicated configured topic, with two
allowlisted groups and at most four members per group. A readable KafkaConsumer
poll/process/commit loop owns each consumer on one thread. The Spring observer
remains independent. Presenter commands alone change worker membership or offsets.

Publish bounded current-state snapshots over the existing topic WebSocket and
retain the latest snapshot for reconnect. Include member identity, assignment,
processing progress, confirmed commits, broker offset samples, and recent events.
Browser navigation never starts workers. Snapshot restoration is not Kafka replay.

Concepts retain onRecord/reset and may implement onExperiment(snapshot). Shared
controls invoke the LiveClient command API; concepts own only rendering. No runtime
plugin loading or navigation lifecycle is introduced. Polling belongs to the
backend, not to mounted slides. This extends ADR-00006's provisional boundary.

The runtime is optional and single-backend, like the existing observer. Its topic
must be pre-created and allowlisted. It does not resize topics. Offset resets are
limited to inactive allowlisted groups and retained offsets. This deliberately
avoids general Kafka administration and leaves production failure semantics to
later lessons.


## Alternatives and consequences

Reusing the observer consumer for experiments would couple record fan-out to
presenter actions and change its membership/commit behavior. Per-slide consumers
or sockets would make navigation part of the experiment. Both conflict with the
established separation of observation and experiment lifecycles.

Dedicated workers add backend state and explicit cleanup responsibilities, while
keeping the teaching loop readable and independently controllable. Bounded
snapshots recover current display state, not a complete event history. Consumers
can continue when no browser is watching; the presenter must explicitly stop them.

[ADR-00008](ADR-00008-shared-group-controls-and-settings.md) extends this decision
with the shared membership widget and backend-owned per-group processing settings.
