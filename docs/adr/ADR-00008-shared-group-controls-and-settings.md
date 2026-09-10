# ADR-00008: Shared group controls and group-scoped experiment settings

Status: Accepted, 2026-09-10

## Context

The group, offset and lag lessons operate on the same experiment runtime. Requiring
presenters to leave the lag slide to change membership interrupts the teaching
loop. A global processing delay also prevents comparing a slow group with a fast
one consuming the same topic.

ADR-00007 establishes independent experiment workers, explicit presenter commands,
and shared observation snapshots. This decision extends that design.

## Decision

- Reuse one `mountGroupMembership(host, { run })` widget across the group, offset
  and lag panels. Keep command controls separate from concept renderers. The widget
  consumes snapshots and delegates commands through the existing LiveClient; it
  owns no socket, poll loop, or Kafka lifecycle.
- Keep selected group and initial-position policy local to each panel. Changing a
  selection only changes the target of subsequent commands. Actual membership and
  processing settings belong to the backend and are shared by all viewers.
- Store processing delay per allowlisted group, keyed by its actual group ID.
  Require a group on delay commands; do not silently fall back to changing both
  groups. Every worker reads its group's delay before each simulated processing
  step, and newly started workers inherit the same setting.
- Retain settings when members stop. Keep settings in memory, initialized to zero
  on backend startup; persistence across application restarts is outside this
  local demo's scope. Delay remains bounded to 0–1000 ms per record.
- Render confirmed settings from snapshots. Display friendly Group A/B labels but
  send actual group IDs. The lag view displays both delays so comparisons remain
  interpretable when a different group is selected in the controls.
- Derive the widget's status from observed worker states and partition assignment
  coverage. This is not a broker group-state query. Missing/stale evidence is
  Unknown, with counts labelled last known; command acceptance is not proof of a
  completed assignment or successful processing.

## Alternatives considered

Duplicating controls in each lesson would allow behavior and error handling to
diverge. A global delay is simpler but cannot demonstrate independent group
progress. Per-member delay would introduce configuration that changes meaning
when members are added or removed; group scope provides a stable teaching target.
Querying broker group state could provide additional evidence, but would add a
new observation source that this compact worker summary does not require.

## Consequences

The same controls are available wherever membership affects the lesson, without
coupling visual modules to Kafka operations. A can accumulate lag while B catches
up, and capacity changes retain the group's processing cost. An in-progress sleep
finishes with its previous delay; changes apply to subsequent processing steps.

The experimental snapshot payload now contains `groupDelays` instead of the former
scalar `delayMs`; the delay command includes `group` and `delayMs`. The current
implementation retains experiment envelope version 1, so this is a breaking shape
change for any older experiment client. Deploy the bundled frontend and backend
together. The original `record-consumed` envelope is unchanged. If independent
clients become a requirement, version incompatible experiment payloads explicitly.

See [the authoring contract](../ADDING_LESSONS.md#shared-group-membership-widget)
for the widget API, and [README](../../README.md#ordering-groups-replay-and-lag-lessons)
for operating commands. Detailed visual layout and preset values belong in those
implementation documents rather than additional ADRs.
