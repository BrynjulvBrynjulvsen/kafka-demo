package io.bekk.kafkademo

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.test.annotation.DirtiesContext
import tools.jackson.databind.JsonNode
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
    "demo.topics=demo-test,other-test", "demo.default-topic=demo-test",
    "spring.kafka.consumer.group-id=kafka-demo-test",
    "demo.experiment.enabled=false",
])
@EmbeddedKafka(partitions = 3, topics = ["demo-test", "other-test"],
    bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class BackendIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired private lateinit var json: ObjectMapper
    @Autowired private lateinit var registry: KafkaListenerEndpointRegistry
    @Autowired private lateinit var broker: EmbeddedKafkaBroker
    private val http = HttpClient.newHttpClient()

    @BeforeEach
    fun awaitConsumer() {
        ContainerTestUtils.waitForAssignment(registry.getListenerContainer("demo-stream")!!, 6)
    }

    @Test
    fun `HTTP production reaches all topic viewers with actual Kafka metadata`() {
        val first = connect("demo-test")
        val second = connect("demo-test")
        val other = connect("other-test")
        try {
            val response = produce("""{"key":"customer-1","value":"hello"}""")
            assertThat(response.statusCode()).isEqualTo(200)
            val sent = json.readTree(response.body())
            val received = json.readTree(first.second.next())
            assertThat(json.readTree(second.second.next())).isEqualTo(received)
            assertThat(received["type"].asText()).isEqualTo("record-consumed")
            assertThat(received["key"].asText()).isEqualTo("customer-1")
            assertThat(received["value"].asText()).isEqualTo("hello")
            assertThat(received["partition"].asInt()).isEqualTo(sent["partition"].asInt())
            assertThat(received["offset"].asLong()).isEqualTo(sent["offset"].asLong())
            assertThat(other.second.messages.poll(300, TimeUnit.MILLISECONDS)).isNull()

            assertThat(produce("""{"topic":"other-test","value":"other"}""").statusCode()).isEqualTo(200)
            assertThat(json.readTree(other.second.next())["value"].asText()).isEqualTo("other")
        } finally {
            listOf(first, second, other).forEach { it.first.sendClose(1000, "done").join() }
        }
    }

    @Test
    fun `consumption commits without viewers and reconnect does not replay discarded messages`() {
        val discarded = json.readTree(produce("""{"value":"no viewers"}""").body())
        awaitCommit(discarded)
        val viewer = connect("demo-test")
        try {
            assertThat(viewer.second.messages.poll(300, TimeUnit.MILLISECONDS)).isNull()
            assertThat(produce("""{"value":"live"}""").statusCode()).isEqualTo(200)
            assertThat(json.readTree(viewer.second.next())["value"].asText()).isEqualTo("live")
        } finally {
            viewer.first.sendClose(1000, "done").join()
        }
    }

    @Test
    fun `invalid production requests are rejected`() {
        assertThat(produce("""{"topic":"not-allowed","value":"hello"}""").statusCode()).isEqualTo(404)
        assertThat(produce("""{"key":"missing-value"}""").statusCode()).isEqualTo(400)
        assertThat(produce(json.writeValueAsString(mapOf("value" to "x".repeat(16385)))).statusCode()).isEqualTo(400)
    }

    @Test
    fun `unknown websocket topics are rejected before upgrading`() {
        try {
            http.newWebSocketBuilder().buildAsync(URI("ws://localhost:$port/ws/topics/not-allowed"), Inbox()).join()
            throw AssertionError("Expected handshake rejection")
        } catch (error: java.util.concurrent.CompletionException) {
            assertThat(error.cause).isInstanceOf(java.net.http.WebSocketHandshakeException::class.java)
            val handshake = error.cause as java.net.http.WebSocketHandshakeException
            assertThat(handshake.response.statusCode()).isEqualTo(404)
        }
    }

    private fun awaitCommit(record: JsonNode) {
        AdminClient.create(mapOf("bootstrap.servers" to broker.brokersAsString)).use { admin ->
            await().atMost(Duration.ofSeconds(15)).untilAsserted {
                val offsets = admin.listConsumerGroupOffsets("kafka-demo-test")
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                assertThat(offsets[TopicPartition("demo-test", record["partition"].asInt())]?.offset())
                    .isEqualTo(record["offset"].asLong() + 1)
            }
        }
    }

    private fun produce(body: String): HttpResponse<String> = http.send(
        HttpRequest.newBuilder(URI("http://localhost:$port/api/messages"))
            .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString(),
    )

    private fun connect(topic: String): Pair<WebSocket, Inbox> {
        val inbox = Inbox()
        val socket = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
            .buildAsync(URI("ws://localhost:$port/ws/topics/$topic"), inbox).get(10, TimeUnit.SECONDS)
        assertThat(json.readTree(inbox.next())["type"].asText()).isEqualTo("subscribed")
        return socket to inbox
    }

    private class Inbox : WebSocket.Listener {
        val messages = LinkedBlockingQueue<String>()
        private val fragments = StringBuilder()
        override fun onOpen(webSocket: WebSocket) { webSocket.request(1) }
        override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            fragments.append(data)
            if (last) {
                messages.add(fragments.toString())
                fragments.setLength(0)
            }
            webSocket.request(1)
            return null
        }
        fun next(): String = messages.poll(10, TimeUnit.SECONDS) ?: throw AssertionError("No WebSocket event received")
    }
}
