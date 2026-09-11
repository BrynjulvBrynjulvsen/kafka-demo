package io.bekk.kafkademo

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = [
    "demo.migration.kubectl=/not-installed/kubectl", "demo.migration.telemetry-topic=migration-telemetry-test",
    "demo.migration.telemetry-group=kafka-demo-migration-integration",
])
@ActiveProfiles("migration")
@EmbeddedKafka(partitions = 1, topics = ["migration-telemetry-test"], bootstrapServersProperty = "demo.migration.telemetry-bootstrap")
@DirtiesContext
class MigrationIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired private lateinit var runtime: MigrationRuntime
    @Autowired private lateinit var broker: EmbeddedKafkaBroker
    @Autowired private lateinit var json: ObjectMapper
    @Autowired private lateinit var registry: KafkaListenerEndpointRegistry

    @Test fun `isolated observer commits without viewers and restores state while Kubernetes is unavailable`() {
        val http = HttpClient.newHttpClient()
        assertThat(registry.getListenerContainer("demo-stream")!!.isRunning).isFalse()
        val post = HttpRequest.newBuilder(URI("http://localhost:$port/api/messages"))
            .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("""{"value":"not allowed"}""")).build()
        assertThat(http.send(post, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404)
        await().atMost(Duration.ofSeconds(30)).until { runtime.snapshot().telemetry.error == null }
        KafkaProducer<String, String>(mapOf("bootstrap.servers" to broker.brokersAsString,
            "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer" to "org.apache.kafka.common.serialization.StringSerializer")).use { producer ->
            producer.send(ProducerRecord("migration-telemetry-test", "p1", """
                {"kind":"consumed-record","group":"g1","member":"c1","topic":"demo.topic","partition":0,"pid":"p1"}
            """.trimIndent())).get(10, TimeUnit.SECONDS)
        }
        await().atMost(Duration.ofSeconds(15)).until { runtime.snapshot().consumedObservations == 1L }
        AdminClient.create(mapOf("bootstrap.servers" to broker.brokersAsString)).use { admin ->
            await().atMost(Duration.ofSeconds(10)).until {
                admin.listConsumerGroupOffsets("kafka-demo-migration-integration").partitionsToOffsetAndMetadata().get()
                    .get(TopicPartition("migration-telemetry-test", 0))?.offset() == 1L
            }
            repeat(2) {
                val inbox = object : WebSocket.Listener {
                    val messages = LinkedBlockingQueue<String>()
                    private val text = StringBuilder()
                    override fun onOpen(ws: WebSocket) { ws.request(1) }
                    override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
                        text.append(data)
                        if (last) { messages.add(text.toString()); text.setLength(0) }
                        ws.request(1); return null
                    }
                }
                val socket = http.newWebSocketBuilder().buildAsync(URI("ws://localhost:$port/ws/migration"), inbox).join()
                try {
                    inbox.messages.poll(5, TimeUnit.SECONDS) // subscribed
                    var received = json.readTree(inbox.messages.poll(5, TimeUnit.SECONDS))
                    if (received.path("consumedObservations").asLong() == 0L)
                        received = json.readTree(inbox.messages.poll(5, TimeUnit.SECONDS))
                    assertThat(received.path("type").asText()).isEqualTo("migration-snapshot")
                    assertThat(received.path("consumedObservations").asLong()).isEqualTo(1)
                    assertThat(received.path("kubernetes").path("error").isNull).isFalse()
                    assertThat(admin.describeConsumerGroups(listOf("kafka-demo-migration-integration")).all().get()
                        .getValue("kafka-demo-migration-integration").members()).hasSize(1)
                } finally { socket.sendClose(1000, "done").join() }
            }
        }
    }
}
