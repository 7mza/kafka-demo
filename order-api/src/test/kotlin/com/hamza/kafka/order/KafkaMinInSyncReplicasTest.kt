package com.hamza.kafka.order

import org.apache.kafka.clients.admin.AdminClient
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaAdmin
import org.testcontainers.DockerClientFactory
import org.testcontainers.junit.jupiter.Testcontainers
import tools.jackson.databind.ObjectMapper
import java.time.Duration

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.min_insync_replicas=3",
        "custom.partitions=3",
        "custom.replication_factor=3",
    ],
)
@Import(PgTestContainer::class, KafkaReplicationTestContainers::class)
class KafkaMinInSyncReplicasTest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    private val broker1 = KafkaReplicationTestContainers.broker1
    private val broker2 = KafkaReplicationTestContainers.broker2
    private val broker3 = KafkaReplicationTestContainers.broker3

    private val brokersByNodeId = mapOf(1 to broker1, 2 to broker2, 3 to broker3)

    private val adminClient by lazy { AdminClient.create(kafkaAdmin.configurationProperties) }

    private val dockerClient = DockerClientFactory.instance().client()

    @BeforeEach
    fun beforeEach() {
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            adminClient.describeTopicPartitions(topicName).first().also {
                assertThat(it.replicas()).hasSize(3)
                assertThat(it.isr()).hasSize(3)
            }
        }
    }

    @AfterEach
    fun afterEach() {
        brokersByNodeId.values.forEach { runCatching { dockerClient.unpauseContainerCmd(it.containerId).exec() } }
    }

    @Test
    fun `no write possible when ISR drops below threshold`() {
        // pausing any 1 node drop ISR to 2 which is below min.insync.replicas=3
        dockerClient.pauseContainerCmd(broker3.containerId).exec()

        // wait for all partitions to evict paused broker from ISR
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            adminClient
                .describeTopicPartitions(topicName)
                .forEach { assertThat(it.isr()).hasSize(2) }
        }

        val outbox =
            Event(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(Item(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
                totalAmountCents = 100,
            ).toOutbox(objectMapper, topicName)

        // broker reject with NotEnoughReplicasException (transient failure)
        // publish should return empty (nothing succeeded) and attempts must not be inc
        assertThat(service.publish(listOf(outbox))).isEmpty()
        assertThat(outbox.attempts).isZero
        assertThat(outbox.publishedAt).isNull()

        // resume paused and check ISR recovered
        dockerClient.unpauseContainerCmd(broker3.containerId).exec()
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(adminClient.describeTopicPartitions(topicName).first().isr()).hasSize(3)
        }
    }
}
