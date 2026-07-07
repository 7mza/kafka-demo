package com.hamza.kafka.inventory

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

@TestConfiguration(proxyBeanMethods = false)
class ProxiedPgTestContainer {
    @Bean
    fun pNetwork(): Network = Network.newNetwork()

    @Bean
    @ServiceConnection
    fun pPgContainer(pNetwork: Network): PostgreSQLContainer =
        PostgreSQLContainer(POSTGRES_IMAGE).withNetwork(pNetwork).withNetworkAliases("pg")

    @Bean
    fun toxiContainer(pNetwork: Network): ToxiproxyContainer =
        ToxiproxyContainer("ghcr.io/shopify/toxiproxy:latest").withNetwork(pNetwork).withExposedPorts(8474, 8666)

    @Bean
    fun proxy(toxiContainer: ToxiproxyContainer): Proxy =
        ToxiproxyClient(toxiContainer.host, toxiContainer.controlPort).createProxy("pg", "0.0.0.0:8666", "pg:5432")

    @Bean
    fun pProperties(
        toxiContainer: ToxiproxyContainer,
        pPgContainer: PostgreSQLContainer,
        proxy: Proxy, // force early proxy creation before datasource connect
    ) = DynamicPropertyRegistrar {
        it.add("spring.datasource.url") {
            "jdbc:postgresql://${toxiContainer.host}:${toxiContainer.getMappedPort(8666)}/${pPgContainer.databaseName}"
        }
    }
}

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

@TestConfiguration(proxyBeanMethods = false)
class KafkaTestContainer {
    @Bean
    open fun kNetwork(): Network = Network.newNetwork()

    @Bean
    @ServiceConnection
    open fun kafkaContainer(kNetwork: Network): KafkaContainer =
        KafkaContainer(KAFKA_IMAGE).withNetwork(kNetwork).withListener("kafka:19092")

    @Bean
    open fun registryContainer(
        kafkaContainer: KafkaContainer,
        kNetwork: Network,
    ): GenericContainer<*> = registryContainer(kNetwork, "PLAINTEXT://kafka:19092", kafkaContainer)

    @Bean
    open fun kProperties(registryContainer: GenericContainer<*>): DynamicPropertyRegistrar =
        DynamicPropertyRegistrar {
            it.add("spring.kafka.consumer.properties.schema.registry.url") { getRegistryUrl(registryContainer) }
        }
}
