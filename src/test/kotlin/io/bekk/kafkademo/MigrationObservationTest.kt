package io.bekk.kafkademo

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import tools.jackson.databind.json.JsonMapper

class MigrationObservationTest {
    private val json = JsonMapper.builder().findAndAddModules().build()

    @Test fun `telemetry keeps independent groups and bounds identities and history`() {
        var time = 1000L
        val state = MigrationState(MigrationProperties(enabled = true)) { time }
        fun record(group: String, kind: String = "consumed-record") = json.readTree("""
            {"kind":"$kind","group":"$group","member":"member","topic":"orders",
            "partition":1,"offset":12,"pid":"producer-1","seq":4}
        """)
        state.telemetry(record("a")); state.telemetry(record("b"))
        assertThat(state.snapshot().members).hasSize(2)
        assertThat(state.snapshot().consumedObservations).isEqualTo(2)
        state.telemetry(record("a", "revoked-partition"))
        assertThat(state.snapshot().members.first { it.group == "a" }.partitions).isEmpty()
        assertThat(state.snapshot().members.first { it.group == "b" }.partitions).containsExactly(1)
        repeat(150) { state.telemetry(record("group-$it", "assigned-partition")) }
        assertThat(state.snapshot().members).hasSize(100)
        assertThat(state.snapshot().events).hasSize(48)
        state.telemetry(json.readTree("""{"kind":"future-event"}"""))
        assertThat(state.snapshot().malformedEvents).isEqualTo(1)
        state.telemetryPoll(true)
        time += 1000
        state.failure("telemetry", "Unavailable")
        assertThat(state.snapshot().telemetry.at).isEqualTo(1000)
        assertThat(state.snapshot().telemetry.error).isEqualTo("Unavailable")
        state.kubernetes(mapOf("selectedVariant" to "legacy"))
        assertThat(state.snapshot().kubernetes.error).isNull()
        assertThat(state.snapshot().telemetry.error).isEqualTo("Unavailable")
    }

    @Test fun `service selection endpoints and configured upstream remain separate and secrets stay out`() {
        val input = json.readTree("""
          {"items":[
            {"kind":"Service","metadata":{"name":"kroxylicious"},"spec":{"selector":{"app":"kroxylicious","variant":"target"}}},
            {"kind":"Deployment","metadata":{"name":"kroxylicious-target"},"spec":{
              "selector":{"matchLabels":{"app":"kroxylicious","variant":"target"}},
              "template":{"metadata":{"labels":{"app":"kroxylicious","variant":"target"}},
                "spec":{"volumes":[{"configMap":{"name":"target-config"}}]}}},"status":{"readyReplicas":1}},
            {"kind":"Pod","metadata":{"name":"target-pod","labels":{"app":"kroxylicious","variant":"target"}}},
            {"kind":"EndpointSlice","metadata":{"labels":{"kubernetes.io/service-name":"kroxylicious"}},
              "endpoints":[{"conditions":{"ready":false},"targetRef":{"name":"target-pod"}}]},
            {"kind":"ConfigMap","metadata":{"name":"target-config"},"data":{"config.yaml":
              "virtualClusters:\n  - name: demo\n    targetCluster:\n      bootstrapServers: target:9097\n      tls:\n        password: VERY_SECRET\n    filters: [block-produce]\n"}}
          ]}
        """)
        val parsed = MigrationKubernetes(MigrationProperties(), json).parse(input)
        val output = json.readTree(json.writeValueAsString(parsed))
        assertThat(output.path("selectedVariant").asText()).isEqualTo("target")
        assertThat(output.path("readyEndpointCount").asInt()).isZero()
        val proxy = output.path("proxies")[0]
        assertThat(proxy.path("selected").asBoolean()).isTrue()
        assertThat(proxy.path("config").path("upstream").asText()).isEqualTo("target:9097")
        assertThat(proxy.path("config").path("filters")[0].asText()).isEqualTo("block-produce")
        assertThat(json.writeValueAsString(parsed)).doesNotContain("VERY_SECRET", "password")
    }

    @Test fun `missing service never implies a legacy route and invalid config remains unknown`() {
        val source = MigrationKubernetes(MigrationProperties(), json)
        val result = source.parse(json.readTree("""{"items":[]}"""))
        assertThat(result["selectedVariant"]).isNull()
        assertThat(result["servicePresent"]).isEqualTo(false)
    }

    @Test fun `old telemetry does not look like fresh client progress`() {
        val state = MigrationState(MigrationProperties()) { 100_000L }
        state.telemetry(json.readTree("""{"kind":"consumed-record","group":"g","member":"m",
            "topic":"t","partition":0,"ts_consume":1000}"""))
        assertThat(state.snapshot().members.single().lastSeen).isEqualTo(100_000L)
        assertThat(state.snapshot().members.single().lastConsumed).isEqualTo(1000L)
    }
    @Test fun `client security settings cannot override observer group or auto commit`() {
        val file = java.nio.file.Files.createTempFile("migration-client", ".properties")
        try {
            java.nio.file.Files.writeString(file, "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\ngroup.id=demo-group\nenable.auto.commit=true\n")
            val properties = telemetryConsumerProperties(MigrationProperties(telemetryClientProperties = file.toString()))
            assertThat(properties["security.protocol"]).isEqualTo("SASL_PLAINTEXT")
            assertThat(properties["sasl.mechanism"]).isEqualTo("SCRAM-SHA-512")
            assertThat(properties["group.id"]).isEqualTo("kafka-demo-migration-observer")
            assertThat(properties["enable.auto.commit"]).isEqualTo(false)
        } finally { java.nio.file.Files.deleteIfExists(file) }
    }

}
