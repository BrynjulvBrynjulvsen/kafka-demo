package io.bekk.kafkademo

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.http.server.ServerHttpRequest
import org.springframework.http.server.ServerHttpResponse
import org.springframework.web.socket.WebSocketHandler
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry
import org.springframework.web.socket.server.HandshakeInterceptor

@Configuration
@EnableWebSocket
class WebSocketConfiguration(
    private val handler: TopicWebSocketHandler,
    private val config: DemoProperties,
) : WebSocketConfigurer {
    @Bean
    fun demoTopicNames(): Array<String> = config.topics.toTypedArray()

    override fun registerWebSocketHandlers(registry: WebSocketHandlerRegistry) {
        registry.addHandler(handler, "/ws/migration")
            .setAllowedOrigins(*config.allowedOrigins.toTypedArray())
            .addInterceptors(object : HandshakeInterceptor {
                override fun beforeHandshake(request: ServerHttpRequest, response: ServerHttpResponse,
                    wsHandler: WebSocketHandler, attributes: MutableMap<String, Any>): Boolean {
                    attributes["topic"] = MIGRATION_CHANNEL
                    return true
                }
                override fun afterHandshake(request: ServerHttpRequest, response: ServerHttpResponse,
                    wsHandler: WebSocketHandler, exception: Exception?) = Unit
            })
        registry.addHandler(handler, "/ws/topics/{topic}")
            .setAllowedOrigins(*config.allowedOrigins.toTypedArray())
            .addInterceptors(object : HandshakeInterceptor {
                override fun beforeHandshake(
                    request: ServerHttpRequest, response: ServerHttpResponse,
                    wsHandler: WebSocketHandler, attributes: MutableMap<String, Any>,
                ): Boolean {
                    val topic = request.uri.path.substringAfterLast('/')
                    if (topic !in config.topics) {
                        response.setStatusCode(HttpStatus.NOT_FOUND)
                        return false
                    }
                    attributes["topic"] = topic
                    return true
                }

                override fun afterHandshake(
                    request: ServerHttpRequest, response: ServerHttpResponse,
                    wsHandler: WebSocketHandler, exception: Exception?,
                ) = Unit
            })
    }
}
