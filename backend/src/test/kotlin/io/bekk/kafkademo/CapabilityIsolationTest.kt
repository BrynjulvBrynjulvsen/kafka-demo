package io.bekk.kafkademo

import io.bekk.kafkademo.core.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.test.context.runner.WebApplicationContextRunner

class CapabilityIsolationTest {
    @Test fun `transport and named Kafka settings need no topics and activate no observer or commands`() {
        WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration::class.java))
            .withUserConfiguration(ObservationTransportConfiguration::class.java, KafkaConnectionsConfiguration::class.java)
            .withPropertyValues("demo.kafka.connections.legacy.properties[bootstrap.servers]=legacy:9092")
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context).hasSingleBean(TopicWebSocketHandler::class.java)
                assertThat(context).doesNotHaveBean(TopicConsumer::class.java)
                assertThat(context).doesNotHaveBean(MessageController::class.java)
                assertThat(context).doesNotHaveBean(DemoProperties::class.java)
                assertThat(context.getBeansOfType(ObservationRoute::class.java)).isEmpty()
                val settings = context.getBean(KafkaConnections::class.java).named("legacy")
                assertThat(settings.clientSettings()["bootstrap.servers"]).isEqualTo("legacy:9092")
                assertThat(settings.clientSettings(mapOf("group.id" to "observer"))["group.id"]).isEqualTo("observer")
            }
    }
}
