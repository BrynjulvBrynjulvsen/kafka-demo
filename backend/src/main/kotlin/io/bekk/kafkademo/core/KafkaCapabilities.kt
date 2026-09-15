package io.bekk.kafkademo.core

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/** These imports activate capabilities; merely depending on the library activates none. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DemoProperties::class)
@Import(MessageController::class)
class KafkaProducerConfiguration

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DemoProperties::class)
@Import(ObservationTransportConfiguration::class, TopicConsumer::class)
class KafkaObserverConfiguration {
    @Bean fun demoTopicNames(config: DemoProperties): Array<String> = config.topics.toTypedArray()
    @Bean fun topicObservationRoute(config: DemoProperties) = ObservationRoute("/ws/topics/{topic}") { request ->
        request.uri.path.substringAfterLast('/').takeIf { it in config.topics }
    }
}
