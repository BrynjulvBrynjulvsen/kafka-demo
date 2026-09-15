package io.bekk.kafkademo.core

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

/** Standard client settings, optionally loaded from a local credentials file. Never publish these. */
class KafkaConnection(
    val properties: Map<String, String> = emptyMap(),
    val clientProperties: String = "",
) {
    fun clientSettings(overrides: Map<String, Any> = emptyMap()): Map<String, Any> {
        val file = Properties()
        if (clientProperties.isNotBlank()) Files.newInputStream(Path.of(clientProperties)).use { file.load(it) }
        return file.stringPropertyNames().associateWith { file.getProperty(it) } + properties + overrides
    }
}

@ConfigurationProperties("demo.kafka")
class KafkaConnections(val connections: Map<String, KafkaConnection> = emptyMap()) {
    fun named(name: String): KafkaConnection = requireNotNull(connections[name]) { "Kafka connection is not configured: $name" }
}

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaConnections::class)
class KafkaConnectionsConfiguration
