package io.bekk.kafkademo

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import tools.jackson.databind.json.JsonMapper
import java.lang.reflect.Proxy
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class TopicWebSocketHandlerTest {
    @Test
    @Timeout(10)
    fun `a blocked viewer cannot block Kafka or a healthy viewer and is disconnected on overflow`() {
        val handler = TopicWebSocketHandler(JsonMapper.builder().findAndAddModules().build())
        val enteredSend = CountDownLatch(1)
        val closed = CountDownLatch(1)
        val received = LinkedBlockingQueue<String>()
        val blocked = session("blocked", onSend = {
            enteredSend.countDown()
            CountDownLatch(1).await() // Simulates a blocking send, interrupted on queue overflow.
        }, onClose = {
            assertThat(it.code).isEqualTo(1008)
            closed.countDown()
        })
        val healthy = session("healthy", onSend = { received.add(it.payload) })
        try {
            handler.afterConnectionEstablished(blocked)
            assertThat(enteredSend.await(2, TimeUnit.SECONDS)).isTrue()
            handler.afterConnectionEstablished(healthy)
            received.poll(2, TimeUnit.SECONDS) // Subscription acknowledgment.
            repeat(65) {
                handler.publish(record(it.toLong()))
                assertThat(received.poll(2, TimeUnit.SECONDS)).contains("record-consumed")
            }
            assertThat(closed.await(2, TimeUnit.SECONDS)).isTrue()
            handler.publish(record(66))
            assertThat(received.poll(2, TimeUnit.SECONDS)).contains("record-consumed", "66")
        } finally {
            handler.shutdown()
        }
    }

    private fun session(
        id: String, onSend: (TextMessage) -> Unit, onClose: (CloseStatus) -> Unit = {},
    ): WebSocketSession = Proxy.newProxyInstance(
        WebSocketSession::class.java.classLoader, arrayOf(WebSocketSession::class.java),
    ) { _, method, args ->
        when (method.name) {
            "getId" -> id
            "isOpen" -> true
            "getAttributes" -> mutableMapOf("topic" to "demo-test")
            "sendMessage" -> { onSend(args!![0] as TextMessage); null }
            "close" -> { onClose(args!![0] as CloseStatus); null }
            else -> throw UnsupportedOperationException(method.name)
        }
    } as WebSocketSession

    private fun record(offset: Long) = ConsumedMessage("demo-test", 0, offset, 0, null, "hello", emptyList())
}
