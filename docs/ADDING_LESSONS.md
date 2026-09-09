# Adding slides and live Kafka lessons

This is the maintained authoring guide for humans and agents. The companion project-local skill is [add-kafka-lesson](../.agents/skills/add-kafka-lesson/SKILL.md). It routes agents here rather than duplicating the implementation contract.

## Choose the smallest extension

| Need | Change |
| --- | --- |
| Explanation, prediction, or code slide | Add a `section` to `src/main/resources/static/index.html` |
| Another view of existing consumed records | Add a concept module and register it in `js/slides.js` |
| New presenter controls | Wire them through the existing `LiveClient`; scope controls to their slide |
| Evidence of processing, commits, batches, or replication | Design the missing backend observations before claiming to display that behavior |

Paths below are relative to `src/main/resources/static` unless stated otherwise. No React, bundler, or runtime plugin loader is required. Spring Boot serves assets and APIs on the same origin. Vendored reveal.js assets are refreshed only for dependency changes; see the root README.

## Plan a lesson

Write down the question learners should predict, the action they will run, the evidence they will see, and the source/configuration they can change. A lesson may span several slides; it does not need a new plugin if prose and an existing illustration suffice.

The current capabilities are:

- `GET /api/topics`: configured allowlist and default, not broker health or partition inventory.
- `POST /api/messages`: string value, optional nullable key, optional allowed topic. Success returns a broker acknowledgment with actual partition and offset.
- `/ws/topics/{topic}`: subscription acknowledgment followed by version-1 `record-consumed` events. Fields include topic, partition, offset, timestamp, nullable key/value, and headers.

The HTTP acknowledgment and consumed event can arrive in either order. A card proves backend consumption was observed; it does not prove processing completion, an offset commit, or replication state. Reconnect has no browser replay. Counts are observations, not unique-record or lifetime Kafka counts. Offsets are partition-local. Null values are tombstones. The current browser client accepts only safe-integer offsets.

## Add an ordinary slide

Insert a section inside `.reveal > .slides`. Use a stable `id` for new slides so links need not depend on position:

```html
<section id="ordering-prediction">
  <p class="eyebrow">PREDICT / ORDERING</p>
  <h2>What does an offset tell us?</h2>
  <p class="intro">Send two records with the same key. Compare their offsets.</p>
  <p class="footnote">Compare records within a partition.</p>
</section>
```

Link to it with `#/ordering-prediction`. Reuse `eyebrow`, `intro`, `footnote`, `predictions`, and `code-layout` styles where appropriate. Update visible slide numbering if the sequence changes. Existing browser tests navigate by position and use `/#/2`; update those tests if insertion changes the experiment's position, preferably switching affected navigation to stable IDs.

## Add a concept plugin

A concept is explicitly imported and registered, then mounted once for each `[data-concept]` root. It owns its local DOM and display state. The shell forwards all valid consumed events to all mounted concepts, including hidden slides. Changing the selected topic resets every concept; manual WebSocket reconnect preserves display state. A concept's local clear control calls its own reset.

The contract is deliberately small:

```js
mountExample(root) => ({ onRecord(record), reset() })
```

Both returned functions are required by the shell. Mounting should initialize the display; reset should safely work repeatedly and should only clear display state. Do not open sockets, start consumers, or produce records inside mount, reset, or onRecord. Handle incoming values as text and keep retained history bounded. Throwing from one concept currently interrupts delivery to later concepts in that event callback, so handle optional payload parsing locally.

For example, this observation counter needs no backend changes. Add the slide:

```html
<section id="observation-count" data-concept="observation-count">
  <p class="eyebrow">OBSERVE / DELIVERY</p>
  <h2>How many observations arrived?</h2>
  <p class="intro"><output data-role="total">0</output> since display reset</p>
  <button type="button" data-action="clear">Clear this count</button>
  <p class="footnote">Produce on the existing lab slide or through the HTTP API.
    This view keeps counting while hidden; reconnect does not replay missed events.</p>
</section>
```

Create `js/concepts/observation-count.js`:

```js
export function mountObservationCount(root) {
  const total = root.querySelector('[data-role="total"]');
  let count = 0;
  function reset() { count = 0; total.textContent = '0'; }
  root.querySelector('[data-action="clear"]').addEventListener('click', reset);
  reset();
  return {
    reset,
    onRecord() { total.textContent = String(++count); },
  };
}
```

Import and register it in `js/slides.js`, retaining the existing entry:

```js
import { mountObservationCount } from './concepts/observation-count.js';
// Extend the existing registry; do not add a second registry or mounting loop.
const concepts = {
  partitioning: mountPartitioning,
  'observation-count': mountObservationCount,
};
```

The example is a copyable recipe, not an installed lesson. New modules should use selectors scoped to `root`, such as `data-role` and `data-action`, rather than global IDs. If options become useful, parse specific `root.dataset` values in the mount function; there is no shared configuration schema yet.

### Current limits to account for

There is one selected topic for the entire deck. Concept modules receive observations from that shared stream, not independent per-slide subscriptions. The current partitioning slide and producer form use singleton IDs; copying that slide verbatim would create duplicate controls and incorrect bindings. Reuse its view by navigating to it, or extract reusable, root-scoped producer controls when a second interactive lab actually needs them. Keep a single application-level `LiveClient` and explicit user-triggered production. Do not build a general plugin framework just to add explanatory slides.

The registry currently lives next to producer wiring in `js/slides.js`. There are no mount/unmount hooks, navigation hooks, or timer disposal contract. Prefer record-driven illustrations; add lifecycle support only when a concrete lesson needs it, and document the extension here.

## Presenter controls and styling

Use the owner's Bekk technology, design and product leadership palette (provided 2026-09-08). CSS tokens in `css/slides.css` are the source of truth: Natt `#1F1F1F`, Dag `#F8F8F8`, Rebell `#D4FF26`; Vann `#909DA9` / `#C6D0D9` / `#EEF3F7`; Jord `#AEB7AB` / `#D9DCCF` / `#F1F1E9`. Data visualization colors also include Datarebell `#C3E835` and Datablå `#0142FE`. Use light Vann for the canvas, Dag for controls and panels, Jord for supporting surfaces, Natt for readable text, Datablå for emphasis/data marks, and Rebell sparingly for the primary action. Keep Ild and Luft (management consulting) out of these slides. Fade older card surfaces rather than making record text illegible through whole-card opacity.

Keep labels explicit about intent and evidence. The existing form maps an empty input to `null`; an empty string sent through the API is a different Kafka key. Do not automatically retry production: a failed acknowledgment can still mean the record was written. Disable overlapping submissions and recover controls after errors.

Preserve keyboard navigation outside form controls. Use labels and native buttons, readable empty/disconnected states, and `textContent` for broker-supplied text. Scope new styles under the concept root or a concept-specific class. The deck uses a 1200 × 700 design canvas scaled by reveal.js; inspect the slide at presentation size and a smaller laptop viewport. `.reveal-viewport` needs the custom background rule because reveal.js supplies its own default background. Avoid CDN/font dependencies for presentation runtime.

## Verify and deliver

Use the root README's build and run commands. Static source changes require a refreshed Spring resource output; a running JAR will not pick up source edits. When testing the JAR, rebuild with `./gradlew bootJar` and restart the existing application rather than accidentally running a second consumer with the same group. Check the actual Docker context and application state first; the Docker build is an alternative when local Java execution is unavailable.

For slide wording/layout changes, inspect the affected rendered slides; no new behavioral test is needed. For concept or control changes, test the new observable behavior plus the affected shared behavior: navigation keeps one socket, topic changes reset state, rendering handles null/text payloads, history remains bounded, and failed production leaves controls usable. Choose the relevant cases instead of repeating every check for every edit. Run backend tests when backend behavior changes.

Existing browser tests are in `tests/browser/slides.spec.js`. `npm ci`, `npx playwright install chromium`, and `npm run test:browser` run them against the application at `http://localhost:8080` (override with `DEMO_URL`). The current suite writes six records per run to the default topic. Wait for backend partition assignment before live tests. Run against the local demo cluster, not an assumed external environment. Distinguish live Kafka verification from mocked API/error tests. Capture and inspect screenshots; passing DOM assertions alone missed a theme contrast bug in the first deck.

After implementing a lesson, update README if run steps or public behavior changed, CONTEXT for the implementation handoff, and this guide if extension mechanics changed. Use an ADR for meaningful architecture changes. Report the entry URL, teaching behavior, validation, and remaining instrumentation limits.

## Planned lesson: batching and sticky partitioning

The owner identified batching as a future standalone lesson. It is not implemented yet.

Start with the prediction “does a null key rotate partitions on every send?” The current default producer uses sticky partition selection for null keys, with `batch.size=16384` observed in the running configuration. Many small records can therefore land in one partition even with pauses between clicks. Verify the actual client version and settings when implementing the lesson; do not freeze this observation into a universal rule.

Partition/offset cards can show the distribution and when it changes. They cannot reveal actual producer batch boundaries, compression, request grouping, or time spent waiting in the producer. If the lesson claims to show those, add appropriate producer metrics or instrumentation. Label any explanatory simulation explicitly. Potential experiments include changing payload volume, `batch.size`, and `linger.ms` one at a time; distinguish the byte threshold for sticky selection from the time limit for sending an incomplete batch. Record the chosen setup and restore changed demo settings afterward as appropriate to the request.
