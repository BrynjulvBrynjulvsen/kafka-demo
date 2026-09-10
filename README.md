# Kafka live demonstrations

A small Kotlin / Spring Boot service that produces UTF-8 string messages and streams consumed records over plain JSON WebSockets. It defaults to the workshop broker advertised at `localhost:9094`.

## Project documentation

- [BACKLOG.md](BACKLOG.md): lesson roadmap and acceptance plans.
- [INTENT.md](INTENT.md): project goals and evolving scope.
- [CONTEXT.md](CONTEXT.md): current implementation, working context, and verification history.
- [Architecture decisions](docs/adr/README.md): accepted decisions and proposals.
- [Adding lessons](docs/ADDING_LESSONS.md): slide and concept recipes, extension boundaries, and verification.
- [Agent skill](.agents/skills/add-kafka-lesson/SKILL.md): reusable lesson-authoring workflow, stored in the repository's skill discovery directory. Invoke `$add-kafka-lesson`; `AGENTS.md` also routes repository agents to it. No personal installation is needed. If it does not appear immediately, restart Codex. See [Codex skill discovery](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills).

## Run

Requires JDK 17 and the workshop Kafka broker. Create the demo topic once in the current Docker context:

If your default Java is newer than the Gradle wrapper supports, set `JAVA_HOME` to a JDK 17 installation first.

```sh
docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 \
  --create --if-not-exists --topic kafka-demo --partitions 3 --replication-factor 1
./gradlew bootRun
```

The backend listens at `http://localhost:8080`. Topics must exist before startup; the consumer does not create topics. Existing topics are never resized by the application. The optional experiment API can reset offsets only for its inactive allowlisted groups.

The workshop Compose stack also includes [Kafbat UI](http://localhost:8081). Open **workshop → Topics → kafka-demo → Messages** and compare a presentation record's topic, partition, offset, key, and value. Kafbat connects to the same broker through `kafka1:9092`; select String deserialization for demo keys and values. See the [workshop instructions](../kafka-workshop/README.md#browse-records-with-kafbat-ui) for startup details.

## Live slides

Open `http://localhost:8080/` after starting the application. The reveal.js deck introduces partitioning, offers predictions, runs a live producer/consumer experiment, and points to the Kotlin code to change.

On the experiment slide, select a configured topic, send several records with `customer-1`, then change the key or leave it empty for a null key. The acknowledgment reports the broker's partition and offset; cards appear only from the separate WebSocket consumption stream. Only observed partitions appear, and each retains four cards. Clear display resets browser observations only.

The deck keeps one WebSocket alive across slide navigation and retries closed connections with capped backoff. Reconnect does not replay missed observations. A subscription confirms the viewer connection, not Kafka partition assignment; wait for the backend's assignment log before presenting. Opening another window adds a viewer, not a Kafka consumer.

Use arrow keys to navigate, Esc for overview, and F for fullscreen. Form controls retain their normal keyboard behavior. Pinned reveal.js 5.2.1 assets and their MIT license are included under `static/vendor/reveal`. The slides need no external network access or frontend build at runtime.

To refresh the bundled library after intentionally changing its version in `package.json`, run `npm install` and `npm run vendor`, then commit the lockfile and vendor assets. For an unchanged lockfile, use `npm ci` instead. Gradle includes the checked-in assets in the application JAR.

The frontend is plain JavaScript served by Spring Boot from `src/main/resources/static`, using same-origin HTTP and WebSocket URLs. `index.html` defines the lesson, `js/slides.js` wires navigation and controls, `js/live-client.js` owns transport, and `js/concepts/partitioning.js` renders observations. New concepts register a mount function that returns `onRecord` and `reset`, with optional `onExperiment` for experiment snapshots. Commands are wired separately in `js/controls`.

## Produce a record with curl

```sh
curl -sS http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' \
  -d '{"key":"customer-1","value":"Hello Kafka"}'
```

An optional `topic` selects another configured topic. Otherwise `KAFKA_DEFAULT_TOPIC` is used. The required `value` is a string (up to 16,384 characters); `key` is optional (up to 1,024 characters). JSON payloads can be encoded inside the string. This version does not use Avro or Schema Registry.

HTTP 200 means Kafka acknowledged the write. The response contains its actual `topic`, `partition`, `offset`, and `timestamp`. Malformed requests return 400, unconfigured topics return 404, and Kafka send failures return 503. A timeout can be ambiguous: the broker may have accepted a record even if the acknowledgment was not received. Retrying an HTTP request may produce a duplicate.

`GET /api/topics` lists the configured topics and default. It is configuration discovery, not a broker health check.

## Subscribe over WebSocket

Open `http://localhost:8080/api/topics` in a browser, then run in its developer console:

```js
const socket = new WebSocket('ws://localhost:8080/ws/topics/kafka-demo');
socket.onmessage = event => console.log(JSON.parse(event.data));
socket.onclose = event => console.log('Stream closed', event.code, event.reason);
```

The first event is `{"type":"subscribed","version":1,"topic":"kafka-demo"}`. This confirms the WebSocket subscription, not Kafka partition assignment. Once the backend consumer has joined its group, subsequent events look like:

```json
{
  "type": "record-consumed",
  "version": 1,
  "topic": "kafka-demo",
  "partition": 0,
  "offset": 42,
  "timestamp": 1788800000000,
  "key": "customer-1",
  "value": "Hello Kafka",
  "headers": []
}
```

Headers preserve their order and duplicate names; binary values use `valueBase64`. Kafka tombstones appear as a null `value`. Partition and offset identify the Kafka record; ordering is only meaningful within each partition.

- One application consumer subscribes to every configured topic at startup. Each WebSocket selects one of those topics by URL. Closing a socket unsubscribes that viewer, without changing Kafka membership.
- Multiple viewers receive copies of the same consumed records. Run one backend instance for this version; instances sharing a group divide partitions, so their viewers would see different subsets.
- With no viewers for a topic, consumed records are logged and discarded from the display pipeline. Batch offset commits continue. This does not delete records from Kafka.
- WebSocket delivery is best-effort, with no replay or client acknowledgment. Reconnecting resumes observations as they arrive. Consumer restarts can still redeliver uncommitted records or process a retained backlog; `latest` applies only when the group has no valid committed offset.
- Each viewer has a queue of 64 events and a dedicated sender. A slow viewer or an event larger than 256 KiB closes that stream with code 1008. At most 32 viewers connect at once. Kafka does not wait on socket sends.

## Configuration

See `.env.example`. Export variables in the shell before starting; Spring does not automatically read that file.

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Workshop broker; comma-separated for multiple brokers |
| `KAFKA_TOPICS` | `kafka-demo` | Topic allowlist consumed at startup |
| `KAFKA_DEFAULT_TOPIC` | `kafka-demo` | Default producer topic; must be in the allowlist |
| `KAFKA_GROUP_ID` | `kafka-demo` | Dedicated group, independent of workshop consumers |
| `PORT` | `8080` | HTTP and WebSocket port |
| `SERVER_ADDRESS` | `127.0.0.1` | Local bind address |
| `WEBSOCKET_ALLOWED_ORIGINS` | localhost:8080, 127.0.0.1:8080, localhost:5173 (HTTP) | Allowed browser origins |

For example, after creating both topics:

```sh
KAFKA_TOPICS=kafka-demo,orders KAFKA_DEFAULT_TOPIC=orders ./gradlew bootRun
```

All standard `spring.kafka.*` properties remain available for security and consumer/producer tuning. The HTTP API has no authentication and defaults to loopback for local workshops. HTTP browser CORS is not enabled; the slides use the backend's origin. When changing `PORT` or the browser hostname, include that origin in `WEBSOCKET_ALLOWED_ORIGINS`.

## Code to teach from

- `MessageController.kt`: KafkaTemplate production and broker acknowledgment metadata.
- `TopicConsumer.kt`: the Kafka listener and consumed-record envelope.
- `DemoProperties.kt` and `application.yml`: topics, consumer group, offsets, and producer settings.
- `TopicWebSocketHandler.kt`: bounded fan-out, disconnection, and no-viewer behavior.
- `WebSocketConfiguration.kt`: topic selection and handshake validation.

## Tests

```sh
./gradlew test
./gradlew bootJar
```

Integration tests run their own embedded KRaft broker and real HTTP/WebSocket server. They cover producer-to-viewer delivery, fan-out, topic isolation, Kafka offsets advancing without viewers, reconnect behavior, and request validation. A separate test exercises a blocked WebSocket sender and queue overflow. They do not depend on or alter the workshop broker.

Browser smoke tests require the running application and its Kafka consumer to have partition assignments. They **produce six records** into the configured default topic, check real acknowledgments against observed cards, bounded display history, literal rendering of HTML-like input, navigation without socket replacement, reconnect, and a simulated HTTP failure:

```sh
npm ci
npx playwright install chromium
npm run test:browser
```

Set `DEMO_URL` to test a different application origin; ensure that origin is allowed by the backend. Screenshots are written to `build/slides-title.png` and `build/slides-live.png`.

To check a running backend against the real workshop broker (produces one message to `kafka-demo`):

```sh
java scripts/SmokeTest.java http://localhost:8080
```

This opens two WebSocket subscriptions, produces via HTTP, and checks both viewers receive the same record with the acknowledged Kafka partition and offset. Wait for the backend's partition assignment log before running it.

### Build and run using Docker

If local JVM execution is restricted, use the same build inside a JDK 17 container:

```sh
docker run --rm \
  -v "$PWD:/workspace" -v kafka-demo-gradle-cache:/home/gradle/.gradle \
  -w /workspace gradle:8.14.3-jdk17 gradle --no-daemon test bootJar

docker run -d --name kafka-demo --network kafkaworkshop \
  -p 127.0.0.1:8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092 -e SERVER_ADDRESS=0.0.0.0 \
  -v "$PWD:/workspace:ro" -w /workspace \
  gradle:8.14.3-jdk17 java -jar build/libs/kafka-demo-0.1.0-SNAPSHOT.jar

docker exec kafka-demo java scripts/SmokeTest.java
docker logs -f kafka-demo
```

These commands use the current Docker context and the workshop's existing `kafkaworkshop` network. The container uses Kafka's internal listener; host execution uses `localhost:9094`. Stop and remove the application container with `docker rm -f kafka-demo` before running it again or using `bootRun` on port 8080. The Gradle cache volume can be reused between builds.


## Ordering, groups, replay and lag lessons

Ordering works with the original setup at `http://localhost:8080/#/ordering`.
The remaining lessons use an opt-in experiment runtime. Create its topic first:

```sh
docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 \
  --create --if-not-exists --topic kafka-demo-lab --partitions 3 --replication-factor 1
KAFKA_TOPICS=kafka-demo,kafka-demo-lab DEMO_EXPERIMENT_ENABLED=true ./gradlew bootRun
```

For the Docker application command above, additionally pass
`-e KAFKA_TOPICS=kafka-demo,kafka-demo-lab -e DEMO_EXPERIMENT_ENABLED=true`.
Use one backend instance and wait for its observer partition assignment before
producing. The runtime does not create topics or start members on its own.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEMO_EXPERIMENT_ENABLED` | `false` | Enable dedicated experiment workers and broker sampling |
| `DEMO_EXPERIMENT_TOPIC` | `kafka-demo-lab` | Pre-created topic, must also appear in KAFKA_TOPICS |
| `DEMO_EXPERIMENT_GROUP_PREFIX` | `kafka-demo-experiment` | Two allowlisted groups, with suffixes `-a` and `-b`; keep separate from observer/workshop groups |

Visit `/#/groups`, click **Observe experiment topic**, and start one member in A.
Add members up to four; with three partitions, one will be idle after assignment.
Start B and send fresh records to compare independent groups. Group membership
continues through navigation and viewer disconnection. **Stop group members** is
explicit; backend shutdown also stops workers.

At `/#/offsets`, stop a group and wait for its members to exit. Pick a partition and
an offset between the sampled Start and End, reset it, then restart. Resets change
one partition only. `earliest`/`latest` is an initial-position policy, used only when
there is no valid commit. To repeat a fresh-group comparison after both groups have
commits, restart with a new dedicated group prefix; a display reset does not erase
commits. Fetch position, processed progress and commit are next-offset markers.

At `/#/lag`, use the previously started workers, apply a processing delay, and send
60 records at 50 ms intervals. Stop production or wait for the bounded workload to
finish, then reduce delay to zero to observe recovery. Delay is selected per group in the membership widget and applies to each of its
workers, including members started later. The workload explicitly cycles through actual topic partitions. It is
capped at 120 records through the API; processing delay is capped at 1000 ms.

`GET /api/experiment` returns current configuration/state. `POST /api/experiment`
accepts `{ "action": "start", "group": "kafka-demo-experiment-a", "policy": "earliest" }`.
Other actions are `stop` (group), `delay` (group, delayMs), `produce` (count, intervalMs),
`stop-production`, and `reset` (group, partition, offset). Concurrent transitions
and invalid operations are rejected or serialized; errors include a message.
Production is never automatically retried after an uncertain acknowledgment.

Experiment snapshots share the topic WebSocket. They restore the latest state on
reconnect and retain at most 48 recent events. Lag charts retain 40 samples. Unknown
commits and stale broker observations are explicit. Zero committed lag does not
prove external business processing; the demo processing step is an intentional
sleep. Consumed-record delivery still has no replay buffer.

`ExperimentRuntime.kt` contains the readable poll/process/commit loop and broker
sampling; `ExperimentController.kt` exposes bounded presenter commands. The three
experiment views are independent modules using optional `onExperiment(snapshot)`.
See [ADR-00007](docs/adr/ADR-00007-controlled-experiment-runtime.md).

The browser suite now also exercises ordering and experiment navigation and needs
this enabled setup. In addition to the original six messages, it sends six ordering
records and a 60-record lab workload. It stops its experiment workers afterward.

Each experiment panel, including lag, now contains the same compact membership
widget. Choose Group A or B, use **+ Add member** or **Stop group**, and inspect the
member count and partition chips in place. **Start position** expands the initial
offset policy. Status summarizes observed workers/assignments; Unknown indicates
stale or missing observations. Group selection changes only the panel's command
target, without creating consumers or switching the shared topic.

Processing delay is now a group setting on all experiment slides. Selecting a delay
applies it immediately to the chosen group; snapshots expose `groupDelays` keyed
by actual group ID. Delay commands require a group. Values remain bounded to
0–1000 ms per record; group settings survive member stops but reset on backend restart.
