package io.bekk.kafkademo

import io.bekk.kafkademo.core.*
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Import

@SpringBootConfiguration
@EnableAutoConfiguration
@Import(KafkaProducerConfiguration::class, KafkaObserverConfiguration::class)
class Application
