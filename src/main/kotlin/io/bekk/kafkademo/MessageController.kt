package io.bekk.kafkademo

import jakarta.validation.Valid
import jakarta.validation.constraints.Size
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.CompletableFuture

data class ProduceMessage(
    @field:Size(max = 16384) val value: String,
    @field:Size(max = 1024) val key: String? = null,
    val topic: String? = null,
)

data class ProducedMessage(val topic: String, val partition: Int, val offset: Long, val timestamp: Long)

@RestController
@RequestMapping("/api")
class MessageController(private val kafka: KafkaTemplate<String, String>, private val config: DemoProperties) {
    @GetMapping("/topics")
    fun topics() = mapOf("topics" to config.topics, "defaultTopic" to config.defaultTopic)

    @PostMapping("/messages")
    fun produce(@Valid @RequestBody message: ProduceMessage): CompletableFuture<ProducedMessage> {
        val topic = config.requireTopic(message.topic ?: config.defaultTopic)
        return try {
            kafka.send(ProducerRecord(topic, message.key, message.value)).handle { result, error ->
                if (error != null) throw unavailable(error)
                result.recordMetadata.let { ProducedMessage(it.topic(), it.partition(), it.offset(), it.timestamp()) }
            }
        } catch (error: Exception) {
            throw unavailable(error)
        }
    }

    private fun unavailable(error: Throwable) =
        ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka did not acknowledge the message", error)
}
