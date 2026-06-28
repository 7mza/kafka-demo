package com.hamza.kafka.order

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.test.context.DynamicPropertyRegistrar
import org.testcontainers.containers.Network
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.lifecycle.Startables
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

@TestConfiguration(proxyBeanMethods = false)
class PgTestContainer {
    @Bean
    @ServiceConnection
    fun pgContainer() = PostgreSQLContainer(DockerImageName.parse("postgres:alpine"))
}

// separate container that can be paused for timeout/error testing without affecting normal tests
@TestConfiguration(proxyBeanMethods = false)
class PausablePgTestContainer {
    @Bean
    @ServiceConnection
    fun pPgContainer() = PostgreSQLContainer(DockerImageName.parse("postgres:alpine"))
}

@TestConfiguration(proxyBeanMethods = false)
class KafkaTestContainer {
    @Bean
    @ServiceConnection
    fun kafkaContainer() = KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
}

// separate container that can be paused for timeout/error testing without affecting normal tests
@TestConfiguration(proxyBeanMethods = false)
class PausableKafkaTestContainer {
    @Bean
    @ServiceConnection
    fun pKafkaContainer() = KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
}

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

        val broker1: KafkaContainer =
            KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
                .withNetwork(network)
                .withNetworkAliases("broker1")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", VOTERS)
                .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "5000")

        val broker2: KafkaContainer =
            KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
                .withNetwork(network)
                .withNetworkAliases("broker2")
                .withEnv("KAFKA_NODE_ID", "2")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", VOTERS)
                .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "5000")

        val broker3: KafkaContainer =
            KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
                .withNetwork(network)
                .withNetworkAliases("broker3")
                .withEnv("KAFKA_NODE_ID", "3")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", VOTERS)
                .withEnv("KAFKA_REPLICA_LAG_TIME_MAX_MS", "5000")

        init {
            /*
             * all nodes must be started concurrently
             * since each ctrl blocks on other ctrls being reachable before it report healthy
             * starting them 1 at a time using @ServiceConnection will deadlock
             */
            Startables.deepStart(broker1, broker2, broker3).join()
        }
    }

    @Bean
    fun properties() =
        DynamicPropertyRegistrar {
            it.add("spring.kafka.bootstrap-servers") {
                "${broker1.bootstrapServers},${broker2.bootstrapServers},${broker3.bootstrapServers}"
            }
        }
}
