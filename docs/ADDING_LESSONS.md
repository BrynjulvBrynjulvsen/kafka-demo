# Extending the Kafka demo core

The original slides and experiment recipes live in
[kafka-lessons](../../kafka-lessons/docs/ADDING_LESSONS.md). Migration-specific views
live in [kafka-migration-demo](../../kafka-migration-demo/docs/ADDING_LESSONS.md).
Use the shared [authoring skill](../.agents/skills/add-kafka-lesson/SKILL.md).

## New demo

Start an independent Spring Boot/Kotlin application with Java 21. Declare dependencies
on `io.bekk.kafkademo:backend:0.1.0-SNAPSHOT` and
`io.bekk.kafkademo:presentation:0.1.0-SNAPSHOT`. Use the existing demos' build files for
matching plugin versions and this settings fragment for local source development:

```kotlin
providers.gradleProperty("kafkaDemoCore").orNull?.let { path ->
    require(file(path).resolve("settings.gradle.kts").isFile) { "Missing Kafka core build: $path" }
    includeBuild(path)
}
```

Give the application its own package outside `io.bekk.kafkademo.core`, entry HTML,
configuration and feature registrations. Do not broadly scan the core package.

## Backend composition

Import `KafkaProducerConfiguration` for `/api/topics` and `/api/messages`, or
`KafkaObserverConfiguration` for `/ws/topics/{topic}` and the observer. Both need
`demo.topics` and `demo.default-topic`; the observer also needs standard `spring.kafka`
consumer settings. Configure allowed origins with `demo.allowed-origins`. No topic
settings are required for transport alone.

For a local feature that publishes snapshots:

```kotlin
@Import(ObservationTransportConfiguration::class)
@Configuration(proxyBeanMethods = false)
class FeatureConfiguration {
    @Bean fun featureRoute() = ObservationRoute("/ws/example") { "/example" }
}
// In a backend-owned runtime, not a viewer or navigation callback:
streams.publishSnapshot("/example", snapshot)
```

Route paths must be unique. A resolver returns a channel or null to reject an unknown
subscription. The version-1 acknowledgment uses `topic` as the logical channel ID for
compatibility. Snapshots restore the latest retained state on reconnect; `publish`
for consumed records does not retain or replay records. Queues are bounded per viewer.
Do not publish raw client settings or credentials. Keep snapshot identity/history
bounded inside the feature, and publish only configured channels.

`KafkaConnectionsConfiguration` binds named `demo.kafka.connections` entries. Each
`KafkaConnection` has `properties` (standard Kafka key/value settings) and optional
`client-properties` (a local file). `clientSettings(overrides)` merges file values,
then explicit properties, then feature-owned overrides. This keeps consumer identity
and commit policy under the feature's control. Construct and close Kafka clients in
the owning runtime; the settings registry opens no network connections.

## Frontend composition

Link `/kafka-demo/vendor/reveal/reveal.css` followed by `/kafka-demo/css/theme.css`
and local feature CSS. Import `initializeDeck`, `mountConcepts`, `dispatchConcepts`
from `/kafka-demo/js/deck.js`. Register view constructors explicitly; constructors
receive their own DOM root. Dispatch isolates renderer exceptions per view.

Use `KafkaClient` from `/kafka-demo/js/kafka-client.js` for basic topics/production and
records, or `LiveClient` from `/kafka-demo/js/live-client.js` for a logical channel:

```js
client.connectChannel({ path: '/ws/example', liveText: 'Example connected' });
client.addEventListener('observation', ({ detail }) => {
  // Validate the feature's payload before dispatching it to local views.
});
```

The optional `channel` connection option filters envelopes by their `topic` field.
Features own payload validation and source freshness; socket status alone does not
prove successful source sampling. Subscribe once per application. Navigation never
reconnects, starts consumers or produces records. Page exit disconnects transport;
restore the connection on persisted page return. Keep controls root-scoped, use
textContent for observations, cap history and support reduced motion.

## Evidence and verification

A consumed record establishes observer delivery, not processing, commit, replication,
or unique-record counts. Those claims require feature-specific instrumentation.
Browser replay is distinct from Kafka replay. Batching and sticky partitioning need
producer evidence before claiming batch boundaries; see the lessons guide.

Core changes need the core tests and affected consumer suites. Demo views need safe
text, bounded state, navigation/reconnect and error/freshness checks, plus visual
inspection at presentation and laptop sizes. Run live tests only against explicitly
selected infrastructure; distinguish fixtures from actual observations. Update this
contract and the ADRs when a shared boundary changes. Never copy machine-specific
permission workarounds into application configuration.
