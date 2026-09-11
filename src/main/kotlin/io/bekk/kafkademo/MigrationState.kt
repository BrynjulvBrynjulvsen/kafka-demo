package io.bekk.kafkademo

import org.springframework.boot.context.properties.ConfigurationProperties
import tools.jackson.databind.JsonNode
import java.util.UUID

const val MIGRATION_CHANNEL = "/migration" // Not a valid Kafka topic; isolated from record delivery.

@ConfigurationProperties("demo.migration")
data class MigrationProperties(
    val enabled: Boolean = false,
    val context: String = "",
    val namespace: String = "kafka-platform-poc",
    val kubectl: String = "kubectl",
    val telemetryBootstrap: String = "localhost:32095",
    val telemetryTopic: String = "telemetry",
    val telemetryGroup: String = "kafka-demo-migration-observer",
    val telemetryClientProperties: String = "",
) {
    init {
        require(namespace.matches(Regex("[a-z0-9][a-z0-9-]{0,62}")))
        require(telemetryGroup.startsWith("kafka-demo-migration-")) { "Use a dedicated kafka-demo-migration-* observer group" }
        require(telemetryTopic.matches(Regex("[a-zA-Z0-9._-]{1,249}")))
    }
}

data class MigrationSource(val at: Long? = null, val error: String? = "Waiting for first observation")
data class MigrationEvent(val id: Long, val at: Long, val source: String, val message: String)
data class MigrationMember(
    val group: String, val member: String, val topic: String,
    val partitions: Set<Int> = emptySet(), val lastSeen: Long,
    val lastConsumed: Long? = null, val observations: Long = 0,
)
data class MigrationProducer(val pid: String, val topic: String, val lastConsumed: Long?, val observations: Long)
data class MigrationSnapshot(
    val enabled: Boolean, val context: String, val namespace: String,
    val kubernetes: MigrationSource, val telemetry: MigrationSource,
    val infrastructure: Map<String, Any?>,
    val members: List<MigrationMember>, val producers: List<MigrationProducer>,
    val events: List<MigrationEvent>, val consumedObservations: Long, val malformedEvents: Long,
    val startedAt: Long, val runId: String, val at: Long,
    val type: String = "migration-snapshot", val version: Int = 1,
)

/** Bounded observations, not an integrity validator or a durable event log. */
class MigrationState(private val config: MigrationProperties, private val now: () -> Long = System::currentTimeMillis) {
    private val startedAt = now()
    private val runId = UUID.randomUUID().toString()
    private var kube = MigrationSource()
    private var telemetry = MigrationSource()
    private var infrastructure = emptyMap<String, Any?>()
    private val members = linkedMapOf<List<String>, MigrationMember>()
    private val producers = linkedMapOf<List<String>, MigrationProducer>()
    private val events = ArrayDeque<MigrationEvent>()
    private var eventId = 0L
    private var observations = 0L
    private var malformed = 0L

    @Synchronized fun kubernetes(value: Map<String, Any?>) {
        val previousRoute = infrastructure["selectedVariant"]
        val route = value["selectedVariant"]
        if (previousRoute != route) event("kubernetes", "Service selector: ${previousRoute ?: "unknown"} → ${route ?: "unknown"}")
        if (infrastructure.isNotEmpty() && infrastructure["proxySignature"] != value["proxySignature"])
            event("kubernetes", "Proxy configuration or ready endpoints changed; inspect topology")
        infrastructure = value
        recovered("kubernetes", kube)
        kube = MigrationSource(now(), null)
    }

    @Synchronized fun failure(source: String, message: String) {
        val previous = if (source == "kubernetes") kube else telemetry
        if (previous.error != message) event(source, message)
        if (source == "kubernetes") kube = previous.copy(error = message) else telemetry = previous.copy(error = message)
    }

    @Synchronized fun telemetryPoll(assigned: Boolean) {
        if (!assigned) {
            failure("telemetry", "Waiting for telemetry partition assignment")
            return
        }
        recovered("telemetry", telemetry)
        telemetry = MigrationSource(now(), null)
    }

    private fun recovered(source: String, previous: MigrationSource) {
        if (previous.error != null) event(source, "Observation source connected")
    }

    @Synchronized fun malformed() {
        malformed++
        if (malformed == 1L || malformed % 100 == 0L) event("telemetry", "$malformed unsupported or malformed telemetry events")
    }

    @Synchronized fun telemetry(node: JsonNode) {
        val kind = node.path("kind").asText("")
        val topic = node.path("topic").asText("").take(249)
        val group = node.path("group").asText("").take(160)
        val member = node.path("member").asText("").take(200)
        val partition = node.path("partition")
        if (kind !in setOf("consumed-record", "assigned-partition", "revoked-partition") ||
            topic.isEmpty() || !partition.isIntegralNumber || partition.asInt(-1) < 0 ||
            group.isEmpty() || member.isEmpty()) { malformed(); return }
        val at = now()
        val key = listOf(group, member, topic)
        val previous = members.remove(key) ?: MigrationMember(group, member, topic, lastSeen = at)
        val p = partition.asInt()
        // A consumed record establishes observation of a partition, not current ownership indefinitely.
        val parts = if (kind == "revoked-partition") previous.partitions - p else (previous.partitions + p).take(64).toSet()
        val consumed = kind == "consumed-record"
        // A retained telemetry backlog must not look like fresh application progress.
        val consumedAt = node.path("ts_consume").takeIf { it.isIntegralNumber && it.asLong() > 0 }?.asLong()
        val lastConsumed = listOfNotNull(previous.lastConsumed, consumedAt.takeIf { consumed }).maxOrNull()
        members[key] = previous.copy(partitions = parts, lastSeen = at,
            lastConsumed = lastConsumed,
            observations = previous.observations + if (consumed) 1 else 0)
        while (members.size > 100) members.remove(members.keys.first())
        if (consumed) {
            observations++
            val pid = node.path("pid").asText("").take(200)
            if (pid.isNotEmpty()) {
                val producerKey = listOf(pid, topic)
                val producer = producers.remove(producerKey)
                producers[producerKey] = MigrationProducer(pid, topic,
                    listOfNotNull(producer?.lastConsumed, consumedAt).maxOrNull(), (producer?.observations ?: 0) + 1)
                while (producers.size > 100) producers.remove(producers.keys.first())
            }
        } else {
            event("telemetry", "${if (kind == "assigned-partition") "Assigned" else "Revoked"} $topic/$p · $group · $member")
        }
    }

    private fun event(source: String, message: String) {
        events.addLast(MigrationEvent(++eventId, now(), source, message.take(600)))
        while (events.size > 48) events.removeFirst()
    }

    @Synchronized fun snapshot() = MigrationSnapshot(config.enabled, config.context, config.namespace,
        kube, telemetry, infrastructure, members.values.toList(), producers.values.toList(), events.toList(),
        observations, malformed, startedAt, runId, now())
}
