package io.bekk.kafkademo

import org.yaml.snakeyaml.LoaderOptions
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor
import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Fixed read-only kubectl invocation; no shell, browser arguments or mutation verbs. */
class MigrationKubernetes(private val config: MigrationProperties, private val json: ObjectMapper) {
    fun sample(): Map<String, Any?> {
        check(config.context.isNotBlank()) { "Set DEMO_MIGRATION_CONTEXT to the local POC Kubernetes context" }
        val process = ProcessBuilder(config.kubectl, "--context", config.context, "--namespace", config.namespace,
            "--request-timeout=5s", "get", "pods,deployments,services,endpointslices,configmaps", "-o", "json")
            .redirectError(ProcessBuilder.Redirect.DISCARD).start()
        val reader = Executors.newSingleThreadExecutor()
        try {
            val output = reader.submit<ByteArray> { process.inputStream.use { it.readNBytes(4 * 1024 * 1024 + 1) } }
            if (!process.waitFor(8, TimeUnit.SECONDS)) error("Kubernetes read timed out")
            check(process.exitValue() == 0) { "Kubernetes read failed; check context, connectivity and read permissions" }
            val bytes = output.get(1, TimeUnit.SECONDS)
            check(bytes.size <= 4 * 1024 * 1024) { "Namespace response exceeds observation limit" }
            return parse(json.readTree(bytes))
        } finally {
            process.destroyForcibly()
            reader.shutdownNow()
        }
    }

    /** Only allowlisted public fields leave the adapter; no raw config, env values or credentials. */
    fun parse(root: JsonNode): Map<String, Any?> {
        require(root.path("items").isArray) { "Invalid Kubernetes list" }
        val items = root.path("items").toList()
        fun name(n: JsonNode) = n.path("metadata").path("name").asText("")
        val service = items.firstOrNull { it.path("kind").asText() == "Service" && name(it) == "kroxylicious" }
        val selector = service?.path("spec")?.path("selector")
        val variant = selector?.path("variant")?.asText("")?.ifBlank { null }
        val workloads = setOf("demo-producer", "demo-consumer", "telemetry-consumer", "demo-streams", "kafka-streams-canary")
        val deployments = items.filter { it.path("kind").asText() == "Deployment" &&
            (name(it) in workloads || name(it).startsWith("kroxylicious-")) }
        val pods = items.filter { it.path("kind").asText() == "Pod" }
        val endpointPods = items.filter { it.path("kind").asText() == "EndpointSlice" &&
            it.path("metadata").path("labels").path("kubernetes.io/service-name").asText() == "kroxylicious" }
            .flatMap { it.path("endpoints").toList() }
            .filter { it.path("conditions").path("ready").asBoolean(false) }
            .map { it.path("targetRef").path("name").asText("") }.filter { it.isNotEmpty() }.toSet()
        val configs = items.filter { it.path("kind").asText() == "ConfigMap" }.associateBy { name(it) }
        val clients = deployments.filter { name(it) in workloads }.map { deployment ->
            val labels = deployment.path("spec").path("selector").path("matchLabels")
            val matching = pods.filter { pod -> matches(labels, pod.path("metadata").path("labels")) }
            mapOf("name" to name(deployment), "desired" to deployment.path("spec").path("replicas").asInt(1),
                "ready" to deployment.path("status").path("readyReplicas").asInt(0),
                "pods" to matching.take(100).map { pod ->
                    val statuses = pod.path("status").path("containerStatuses").toList()
                    mapOf("name" to name(pod), "uid" to pod.path("metadata").path("uid").asText(""),
                        "phase" to pod.path("status").path("phase").asText("Unknown"),
                        "ready" to (statuses.isNotEmpty() && statuses.all { it.path("ready").asBoolean(false) }),
                        "restarts" to statuses.sumOf { it.path("restartCount").asInt(0) })
                }, "omittedPods" to (matching.size - 100).coerceAtLeast(0))
        }
        val proxies = deployments.filter { name(it).startsWith("kroxylicious-") }.take(6).map { deployment ->
            val labels = deployment.path("spec").path("template").path("metadata").path("labels")
            val selected = selector != null && matches(selector, labels)
            val mountedConfigs = deployment.path("spec").path("template").path("spec").path("volumes").toList()
                .map { it.path("configMap").path("name").asText("") }
            val configName = mountedConfigs.firstOrNull { configs[it]?.path("data")?.has("config.yaml") == true }
            val raw = configName?.let { configs[it]?.path("data")?.path("config.yaml")?.asText("") }
            val publicConfig = raw?.let { publicConfig(it) } ?: mapOf("configError" to "Configuration unavailable")
            val matchingPods = pods.filter { matches(deployment.path("spec").path("selector").path("matchLabels"), it.path("metadata").path("labels")) }
            mapOf("name" to name(deployment), "variant" to labels.path("variant").asText("unknown"),
                "selected" to selected, "ready" to deployment.path("status").path("readyReplicas").asInt(0),
                "readyEndpoints" to matchingPods.count { name(it) in endpointPods },
                "configName" to configName, "config" to publicConfig)
        }
        return mapOf("selectedVariant" to variant, "servicePresent" to (service != null),
            "readyEndpointCount" to endpointPods.size, "clients" to clients, "proxies" to proxies,
            "proxySignature" to proxies.toString().hashCode().toString())
    }

    private fun matches(selector: JsonNode, labels: JsonNode): Boolean = selector.isObject && !selector.isEmpty &&
        selector.properties().all { (key, value) -> labels.path(key) == value }

    private fun publicConfig(raw: String): Map<String, Any?> = runCatching {
        val yaml = Yaml(SafeConstructor(LoaderOptions().apply { codePointLimit = 128 * 1024; maxAliasesForCollections = 10 }))
        val doc = yaml.load<Any>(raw) as? Map<*, *> ?: error("Invalid config")
        val virtual = (doc["virtualClusters"] as? List<*>)?.firstOrNull() as? Map<*, *>
        val target = virtual?.get("targetCluster") as? Map<*, *>
        val upstream = target?.get("bootstrapServers")?.toString()?.take(200)
        val filters = (virtual?.get("filters") as? List<*>)?.take(10)?.map { it.toString().take(80) } ?: emptyList()
        mapOf("upstream" to upstream, "filters" to filters, "upstreamTls" to (target?.get("tls") != null),
            "hash" to MessageDigest.getInstance("SHA-256").digest(raw.toByteArray()).take(6).joinToString("") { "%02x".format(it) })
    }.getOrElse { mapOf("configError" to "Configuration could not be parsed") }
}
