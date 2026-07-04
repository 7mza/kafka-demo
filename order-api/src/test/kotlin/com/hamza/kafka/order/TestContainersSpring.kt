package com.hamza.kafka.order

import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.test.context.DynamicPropertyRegistrar
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.toxiproxy.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName

private val KAFKA_IMAGE = DockerImageName.parse("apache/kafka:latest")
private val POSTGRES_IMAGE = DockerImageName.parse("postgres:alpine")

@TestConfiguration(proxyBeanMethods = false)
class PgTestContainer {
    @Bean
    @ServiceConnection
    fun pgContainer(): PostgreSQLContainer = PostgreSQLContainer(POSTGRES_IMAGE)
}

@Suppress("SpringJavaInjectionPointsAutowiringInspection")
@TestConfiguration(proxyBeanMethods = false)
class ProxiedPgTestContainer {
    @Bean
    fun network(): Network = Network.newNetwork()

    @Bean
    fun pgContainer(network: Network): PostgreSQLContainer =
        PostgreSQLContainer(POSTGRES_IMAGE).withNetwork(network).withNetworkAliases("pg")

    @Bean
    fun toxiContainer(network: Network): ToxiproxyContainer =
        ToxiproxyContainer("ghcr.io/shopify/toxiproxy:latest").withNetwork(network).withExposedPorts(8474, 8666)

    @Bean
    fun proxy(toxiContainer: ToxiproxyContainer): Proxy =
        ToxiproxyClient(toxiContainer.host, toxiContainer.controlPort).createProxy("pg", "0.0.0.0:8666", "pg:5432")

    @Bean
    fun properties(
        toxiContainer: ToxiproxyContainer,
        pgContainer: PostgreSQLContainer,
        proxy: Proxy, // force early proxy creation before datasource connect
    ) = DynamicPropertyRegistrar {
        it.add("spring.datasource.url") {
            "jdbc:postgresql://${toxiContainer.host}:${toxiContainer.getMappedPort(8666)}/${pgContainer.databaseName}"
        }
        it.add("spring.datasource.username") { pgContainer.username }
        it.add("spring.datasource.password") { pgContainer.password }
    }
}

private fun kafkaContainer(network: Network): KafkaContainer =
    KafkaContainer(KAFKA_IMAGE).withNetwork(network).withListener("kafka:19092")

private fun registryContainer(
    network: Network,
    bootstrap: String,
    vararg dependsOn: KafkaContainer,
): GenericContainer<*> =
    GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:latest"))
        .withNetwork(network)
        .withExposedPorts(8081)
        .dependsOn(*dependsOn)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrap)
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))

private fun getRegistryUrl(container: GenericContainer<*>) = "http://${container.host}:${container.getMappedPort(8081)}"

@Suppress("SpringJavaInjectionPointsAutowiringInspection")
abstract class BaseKafkaTestContainer {
    @Bean
    open fun network(): Network = Network.newNetwork()

    @Bean
    @ServiceConnection
    open fun kafkaContainer(network: Network): KafkaContainer =
        com.hamza.kafka.order
            .kafkaContainer(network)

    @Bean
    open fun registryContainer(
        kafkaContainer: KafkaContainer,
        network: Network,
    ): GenericContainer<*> = registryContainer(network, "PLAINTEXT://kafka:19092", kafkaContainer)

    @Bean
    open fun properties(registryContainer: GenericContainer<*>): DynamicPropertyRegistrar =
        DynamicPropertyRegistrar {
            it.add("spring.kafka.producer.properties.schema.registry.url") { getRegistryUrl(registryContainer) }
        }
}

@TestConfiguration(proxyBeanMethods = false)
class KafkaTestContainer : BaseKafkaTestContainer()

@TestConfiguration(proxyBeanMethods = false)
class PausableKafkaTestContainer : BaseKafkaTestContainer()

/*
 * Kraft cluster with 3 nodes each acting as ctrl + broker
 *  ctrl = which node is managing cluster metadata
 * ctrl quorum election is based on majority vote : `floor(N/2) + 1` where N is number of ctrls
 * with 2 : `floor(2/2) + 1 = 2` election will hang if not both ctrls are alive (1 out of 2 is not majority)
 * with 3 : `floor(3/2) + 1 = 2` election will work if 1 fail (2 out of 3 is majority)
 * for election to work = more than half electors need to be alive
 */
@TestConfiguration(proxyBeanMethods = false)
class KafkaReplicationTestContainers {
    companion object {
        private val network = Network.newNetwork()
        private const val VOTERS = "1@broker1:9094,2@broker2:9094,3@broker3:9094"

        private fun broker(id: Int): KafkaContainer =
            KafkaContainer(KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("broker$id")
                .withEnv("KAFKA_NODE_ID", "$id")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", VOTERS)
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
                .withEnv("KAFKA_BROKER_HEARTBEAT_INTERVAL_MS", "1000")
                .withEnv("KAFKA_BROKER_SESSION_TIMEOUT_MS", "6000")
                .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "10000")
                .withListener("kafka$id:19092")

        val broker1: KafkaContainer = broker(1)
        val broker2: KafkaContainer = broker(2)
        val broker3: KafkaContainer = broker(3)
        var registry: GenericContainer<*> =
            registryContainer(network, "PLAINTEXT://kafka1:19092,kafka2:19092,kafka3:19092", broker1, broker2, broker3)

        init {
            /*
             * all nodes must be started concurrently
             * since each ctrl blocks on other ctrls being reachable before it report healthy
             * starting them 1 at a time using @ServiceConnection will deadlock
             */
            Startables.deepStart(broker1, broker2, broker3, registry).join()
        }
    }

    @Bean
    fun properties() =
        DynamicPropertyRegistrar {
            it.add("spring.kafka.bootstrap-servers") {
                "${broker1.bootstrapServers},${broker2.bootstrapServers},${broker3.bootstrapServers}"
            }
            it.add("spring.kafka.producer.properties.schema.registry.url") { getRegistryUrl(registry) }
        }
}
