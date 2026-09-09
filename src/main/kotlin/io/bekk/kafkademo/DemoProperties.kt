package io.bekk.kafkademo

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException

@ConfigurationProperties("demo")
data class DemoProperties(
    val topics: List<String>,
    val defaultTopic: String,
    val allowedOrigins: List<String>,
) {
    init {
        require(topics.isNotEmpty() && topics.all {
            it.matches(Regex("[a-zA-Z0-9._-]{1,249}")) && it != "." && it != ".."
        }) { "demo.topics must contain valid Kafka topic names" }
        require(defaultTopic in topics) { "demo.default-topic must be in demo.topics" }
    }

    fun requireTopic(topic: String): String {
        if (topic !in topics) throw ResponseStatusException(HttpStatus.NOT_FOUND, "Topic is not configured")
        return topic
    }
}
