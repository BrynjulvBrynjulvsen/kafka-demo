package io.bekk.kafkademo

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Component
class MigrationRuntime(
    private val config: MigrationProperties, private val json: ObjectMapper,
    private val streams: TopicWebSocketHandler,
) {
    private val state = MigrationState(config)
    private val scheduler = Executors.newScheduledThreadPool(2) { task ->
        Thread(task, "migration-observer").apply { isDaemon = true }
    }
    @Volatile private var running = false
    @Volatile private var consumer: KafkaConsumer<String, String>? = null
    private var worker: Thread? = null

    @PostConstruct fun start() {
        streams.publishSnapshot(MIGRATION_CHANNEL, state.snapshot())
        if (!config.enabled) return
        running = true
        val kubernetes = MigrationKubernetes(config, json)
        scheduler.scheduleWithFixedDelay({
            try { state.kubernetes(kubernetes.sample()) }
            catch (error: Exception) {
                state.failure("kubernetes", if (error is IllegalStateException) error.message ?: "Kubernetes read failed"
                    else "Kubernetes read failed (${error.javaClass.simpleName}); check kubectl and context")
            }
        }, 0, 3, TimeUnit.SECONDS)
        scheduler.scheduleWithFixedDelay({ streams.publishSnapshot(MIGRATION_CHANNEL, state.snapshot()) }, 0, 1, TimeUnit.SECONDS)
        worker = Thread({ observeTelemetry() }, "migration-telemetry").apply { isDaemon = true; start() }
    }

    private fun observeTelemetry() {
        while (running) {
            try {
                KafkaConsumer<String, String>(telemetryConsumerProperties(config)).use { kafka ->
                    consumer = kafka
                    kafka.subscribe(listOf(config.telemetryTopic))
                    var nextProbeAt = 0L
                    while (running) {
                        val records = kafka.poll(Duration.ofSeconds(1))
                        records.forEach { record ->
                            if (record.value() == null || record.value().length > 16_384) state.malformed()
                            else try { state.telemetry(json.readTree(record.value())) }
                            catch (_: Exception) { state.malformed() }
                        }
                        if (!records.isEmpty) kafka.commitSync(Duration.ofSeconds(3))
                        val assignment = kafka.assignment()
                        if (assignment.isEmpty()) state.telemetryPoll(false)
                        else if (System.currentTimeMillis() >= nextProbeAt) {
                            val ends = kafka.endOffsets(assignment, Duration.ofSeconds(3))
                            check(ends.keys.containsAll(assignment)) { "Incomplete telemetry broker sample" }
                            state.telemetryPoll(true)
                            nextProbeAt = System.currentTimeMillis() + 3000
                        }
                    }
                }
            } catch (_: WakeupException) {
                if (running) state.failure("telemetry", "Telemetry observer interrupted")
            } catch (error: Exception) {
                state.failure("telemetry", "Telemetry read failed (${error.javaClass.simpleName}); check legacy Kafka connectivity and client security properties")
            } finally { consumer = null }
            if (running) try { Thread.sleep(3000) } catch (_: InterruptedException) { return }
        }
    }

    fun snapshot(): MigrationSnapshot = state.snapshot()

    @PreDestroy fun stop() {
        running = false
        scheduler.shutdownNow()
        consumer?.wakeup()
        worker?.join(8000)
    }
}

/** Standard Kafka client security settings; observer identity and read-only behavior stay fixed. */
internal fun telemetryConsumerProperties(config: MigrationProperties): Map<String, Any> {
    val security = Properties()
    if (config.telemetryClientProperties.isNotBlank()) {
        Files.newInputStream(Path.of(config.telemetryClientProperties)).use { security.load(it) }
    }
    return security.stringPropertyNames().associateWith { security.getProperty(it) } + mapOf(
        "bootstrap.servers" to config.telemetryBootstrap, "group.id" to config.telemetryGroup,
        "client.id" to "kafka-demo-migration-telemetry",
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "auto.offset.reset" to "latest", "enable.auto.commit" to false,
        "allow.auto.create.topics" to false, "max.poll.records" to 250,
        "default.api.timeout.ms" to 5000, "request.timeout.ms" to 5000,
    )
}

@RestController
class MigrationController(private val runtime: MigrationRuntime) {
    @GetMapping("/api/migration") fun snapshot(): MigrationSnapshot = runtime.snapshot()
}
