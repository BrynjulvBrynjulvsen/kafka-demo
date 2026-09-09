package io.bekk.kafkademo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.Base64

data class RecordHeader(val key: String, val valueBase64: String?)
data class ConsumedMessage(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val key: String?,
    val value: String?,
    val headers: List<RecordHeader>,
    val type: String = "record-consumed",
    val version: Int = 1,
)

@Component
class TopicConsumer(private val streams: TopicWebSocketHandler) {
    @KafkaListener(id = "demo-stream", topics = ["#{@demoTopicNames}"], idIsGroup = false)
    fun consume(record: ConsumerRecord<String, String>) {
        streams.publish(ConsumedMessage(
            topic = record.topic(), partition = record.partition(), offset = record.offset(),
            timestamp = record.timestamp(), key = record.key(), value = record.value(),
            headers = record.headers().map {
                RecordHeader(it.key(), it.value()?.let(Base64.getEncoder()::encodeToString))
            },
        ))
    }
}
