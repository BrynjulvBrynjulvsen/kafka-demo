package io.bekk.kafkademo

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.adapter.NativeWebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

/** One bounded queue and sender per viewer; Kafka consumption never waits on socket I/O. */
@Component
class TopicWebSocketHandler(private val json: ObjectMapper) : TextWebSocketHandler() {
    private val log = LoggerFactory.getLogger(javaClass)
    private val viewers = ConcurrentHashMap<String, Viewer>()
    private val experiments = ConcurrentHashMap<String, TextMessage>()

    private class Viewer(val session: WebSocketSession, val topic: String) {
        val queue = ArrayBlockingQueue<TextMessage>(64)
        lateinit var sender: Thread
        @Volatile var closeStatus = CloseStatus.NORMAL
    }

    @Synchronized
    override fun afterConnectionEstablished(session: WebSocketSession) {
        if (viewers.size >= 32) {
            session.close(CloseStatus(1013, "Too many viewers; try again later"))
            return
        }
        // Bound blocking writes in the embedded Tomcat WebSocket implementation.
        (session as? NativeWebSocketSession)?.getNativeSession(jakarta.websocket.Session::class.java)
            ?.userProperties?.put("org.apache.tomcat.websocket.BLOCKING_SEND_TIMEOUT", 5000L)
        val viewer = Viewer(session, session.attributes["topic"] as String)
        viewer.sender = Thread({ sendMessages(viewer) }, "websocket-${session.id}").apply { isDaemon = true }
        viewer.queue.add(TextMessage(json.writeValueAsString(mapOf(
            "type" to "subscribed", "version" to 1, "topic" to viewer.topic,
        ))))
        experiments[viewer.topic]?.let { viewer.queue.add(it) }
        viewers[session.id] = viewer
        viewer.sender.start()
    }

    fun publish(record: ConsumedMessage) {
        val subscribers = viewers.values.filter { it.topic == record.topic }
        if (subscribers.isEmpty()) {
            log.info("No viewers; discarding consumed record topic={} partition={} offset={} key={} value={}",
                record.topic, record.partition, record.offset, record.key, record.value)
            return
        }
        val message = TextMessage(json.writeValueAsString(record))
        subscribers.forEach { viewer ->
            if (message.payloadLength > 256 * 1024 || !viewer.queue.offer(message)) {
                // Closing happens on the sender, never on Kafka's listener thread.
                viewer.closeStatus = CloseStatus.POLICY_VIOLATION.withReason("Viewer cannot keep up; reconnect for live events")
                viewers.remove(viewer.session.id, viewer)
                viewer.sender.interrupt()
            }
        }
    }

    fun publishExperiment(topic: String, snapshot: Map<String, Any?>) {
        publishSnapshot(topic, snapshot)
    }

    /** Logical channels can carry snapshots independently of Kafka topic subscriptions. */
    fun publishSnapshot(topic: String, snapshot: Any) {
        val message = TextMessage(json.writeValueAsString(snapshot))
        experiments[topic] = message
        viewers.values.filter { it.topic == topic }.forEach { viewer ->
            if (message.payloadLength > 256 * 1024 || !viewer.queue.offer(message)) {
                viewer.closeStatus = CloseStatus.POLICY_VIOLATION.withReason("Viewer cannot keep up")
                viewers.remove(viewer.session.id, viewer)
                viewer.sender.interrupt()
            }
        }
    }

    private fun sendMessages(viewer: Viewer) {
        try {
            while (!Thread.currentThread().isInterrupted && viewer.session.isOpen) {
                viewer.session.sendMessage(viewer.queue.take())
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (error: Exception) {
            log.debug("WebSocket sender stopped: {}", error.message)
            if (viewer.closeStatus == CloseStatus.NORMAL) viewer.closeStatus = CloseStatus.SERVER_ERROR
        } finally {
            viewers.remove(viewer.session.id, viewer)
            viewer.queue.clear()
            runCatching { viewer.session.close(viewer.closeStatus) }
        }
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        viewers.remove(session.id)?.sender?.interrupt()
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        viewers.remove(session.id)?.let {
            it.closeStatus = CloseStatus.SERVER_ERROR
            it.sender.interrupt()
        }
    }

    @PreDestroy
    fun shutdown() {
        viewers.values.toList().forEach { it.sender.interrupt() }
        viewers.clear()
    }
}
