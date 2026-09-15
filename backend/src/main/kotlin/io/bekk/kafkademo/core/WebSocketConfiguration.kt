package io.bekk.kafkademo.core

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.server.ServerHttpRequest
import org.springframework.http.server.ServerHttpResponse
import org.springframework.web.socket.WebSocketHandler
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry
import org.springframework.web.socket.server.HandshakeInterceptor

@ConfigurationProperties("demo")
data class TransportProperties(val allowedOrigins: List<String> = emptyList())

/** Applications explicitly register routes. Resolving a viewer never starts a Kafka client. */
class ObservationRoute(val path: String, val channel: (ServerHttpRequest) -> String?)

@Configuration(proxyBeanMethods = false)
@EnableWebSocket
@EnableConfigurationProperties(TransportProperties::class)
@Import(TopicWebSocketHandler::class)
class ObservationTransportConfiguration(
    private val handler: TopicWebSocketHandler,
    private val config: TransportProperties,
    private val routes: List<ObservationRoute>,
) : WebSocketConfigurer {
    override fun registerWebSocketHandlers(registry: WebSocketHandlerRegistry) {
        require(routes.map { it.path }.distinct().size == routes.size) { "Observation routes must be unique" }
        routes.forEach { route ->
            registry.addHandler(handler, route.path)
                .setAllowedOrigins(*config.allowedOrigins.toTypedArray())
                .addInterceptors(object : HandshakeInterceptor {
                    override fun beforeHandshake(request: ServerHttpRequest, response: ServerHttpResponse,
                        wsHandler: WebSocketHandler, attributes: MutableMap<String, Any>): Boolean {
                        val channel = route.channel(request)
                        if (channel == null) { response.setStatusCode(HttpStatus.NOT_FOUND); return false }
                        attributes["topic"] = channel // Preserve the version-1 subscription envelope.
                        return true
                    }
                    override fun afterHandshake(request: ServerHttpRequest, response: ServerHttpResponse,
                        wsHandler: WebSocketHandler, exception: Exception?) = Unit
                })
        }
    }
}
