# Kroxy migration demonstration

Plan accepted 2026-09-10. Source POC: sibling `kafka-proxy-poc`.

## Goal and scope

Present the Kroxylicious + Confluent Cluster Linking migration using the existing
reveal.js presentation style. Exclude MM2 and Grepplabs. The POC owns infrastructure
and migration operations; kafka-demo owns presentation and observation.

Run setup with `poc-setup/kroxy/setup-kroxy-poc.sh`, migration with
`helper-scripts/kroxy/migrate-to-target.sh`, and local Kafka access with
`helper-scripts/common/forward-kafka-ports.sh` in the POC checkout.

## Presentation and architecture

- Separate migration deck, reusing local reveal.js, palette, rendering conventions,
  and one browser connection. Navigation never starts observers or migration work.
- Live topology: clients → stable endpoint → selected Kroxy variant → legacy/target;
  a separate legacy → target edge represents Cluster Linking.
- Client summary: Kubernetes lifecycle and restarts separately from application
  progress, assignments, acknowledgments, and consumption.
- Proxy summary: Service selector, ready endpoints, deployment, upstream and filters.
  Configured routing is not proof that existing connections moved. ConfigMap contents
  are not proof of the revision loaded by a running proxy.
- Cluster evidence: topic activity, mirror state/lag, and group offsets. Replicated
  records appearing on target are not proof of direct client writes there.
- Bounded timeline: assignments/revocations, configuration transitions, write pause,
  failover, recovery and sequence anomalies, each with source and observation time.
- Backend migration runtime combines independent Kubernetes, telemetry, Kafka Admin,
  and Cluster Linking sources. Versioned snapshots restore current state on reconnect;
  source freshness and errors stay independent. No browser-driven Kafka membership.
- Local access uses legacy localhost:32095 and target localhost:32096. A later
  in-cluster backend could use internal listeners and namespace-scoped read access.

## 1. Read-only first increment — implemented and live-verified

- [x] Persist this plan and add a separately addressable migration deck.
- [x] Observe Kubernetes workloads, Service routing, endpoints and proxy configuration.
- [x] Consume existing legacy telemetry with a separate observer group; retain bounded
  client/group progress and important assignment/revocation events.
- [x] Render live topology, client summary, and event timeline through one shared socket.
- [x] Clearly label unknown cluster traffic, producer progress, loaded config, and link
  state where the existing sources do not establish them.
- [x] Keep existing POC scripts as presenter controls in a terminal; no mutation API.
- [x] Verify parsing/state behavior, error/freshness handling, safe rendering, bounded
  retention, navigation/reconnect, and build. Record live versus fixture verification.
- [x] Document configuration, run steps, evidence limits and extension contract.


- [x] Verify both sources against the running POC in `kind-kafka-proxy-poc`.
  Verified 2026-09-11 using the exported kubeconfig and external-listener SASL settings.
  Live browser navigation/reconnect and the dedicated observer group were also checked.

## 2. Stronger client and integrity telemetry

- Pod UID, process/run identity, resolved topic/group and periodic heartbeat.
- Producer acknowledgment/error events (not the pre-send log message).
- Correlate consumer member identity to Kubernetes pods explicitly.
- Inspect the external Streams canary's interfaces; default setup deploys it rather
  than this POC's `StreamsApp.kt` deployment.
- Compare acknowledged sequences with consumption per group. Current telemetry
  deduplication by topic/partition/offset omits group/cluster identity and cannot
  substantiate a general zero-loss/zero-duplicates badge. Bound tracker memory.
- Treat gaps as provisional until reconciliation; make coverage and run boundaries clear.

## 3. Cluster and migration evidence

- Direct per-cluster Kafka Admin samples and optionally header-only record observers.
- Structured Cluster Linking samples with complete topic/partition coverage, mirror
  lag/state and source/target committed offsets; missing samples remain unknown.
- Structured script phase started/completed/failed events, with migration run ID.
  Script intent stays separate from independently observed completion.
- Proxy startup configuration hash for loaded-revision evidence; optional request metrics
  for blocking/retry behavior and upstream attribution.
- Tighten POC script checks before showing green migration completion gates: current
  script uses `--failover` despite README promotion wording, checks presence of sampled
  target offsets rather than source/target equality, and treats empty lag output as zero.

## 4. Guided presentation

Predict interruption → observe steady traffic → block writes → inspect replication
and offsets → fail over → switch route → measure recovery. Show acknowledgment
interruption, client pod continuity/restarts, resumed consumption and unresolved gaps.
Only add explicit presenter controls after observations are dependable; preserve the
curated POC migration script as the operational authority.

## Evidence boundaries

Telemetry currently contains consumed-record, assigned-partition and revoked-partition
events, producer sequence, group/member, topic/partition/offset and timestamps. It goes
directly to legacy Kafka and remains dependent on legacy after cutover. There are no
producer acknowledgments, client cluster identity, or migration milestones in that schema.
Pod readiness is not application progress. Source review is not live runtime verification.

## Follow-up from live connection work

- The external legacy listener uses SASL_PLAINTEXT / SCRAM-SHA-512, not PLAINTEXT.
  The observer now accepts a local Kafka client properties file for authentication.
- The owner confirmed the Streams canary is nonessential and to be removed; ignore
  its image-pull failure for acceptance. Remove it from the POC in a later cleanup.

## Visual flow increment — implemented 2026-09-11

- Added a dedicated flow stage at `#/migration-flow` with pod lights, selected proxy
  and broker route, produce-block configuration, consumer-group activity, received
  report rate and bounded consumption pulses.
- Pulses use new telemetry observations on a schematic return path; broker routing
  is configured-state evidence only. Producer acknowledgments and replication
  traffic still require the instrumentation in steps 2–3 above.
- Reconnected the observer following reset; verified live delivery and browser
  freshness/reconnect/reduced-motion behavior.
