package com.hamza.kafka.order

import com.hamza.kafka.commons.parseJson
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.testcontainers.DockerClientFactory
import org.testcontainers.junit.jupiter.Testcontainers
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.concurrent.TimeUnit

fun AdminClient.describeTopicPartitions(topicName: String): List<TopicPartitionInfo> =
    describeTopics(listOf(topicName))
        .topicNameValues()[topicName]!!
        .get(5, TimeUnit.SECONDS)
        .partitions()

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.orders.min_insync_replicas=2",
        "custom.orders.partitions=3",
        "custom.orders.replication_factor=3",
        "custom.orders.publish_timeout=PT1M", // loosen timeout
        "custom.orders.topic_name=replication-test",
        "spring.kafka.producer.properties.metadata.max.age.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=2000",
    ],
)
@Import(PgTestContainer::class, KafkaReplicationTestContainers::class)
class KafkaReplicationTest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Value($$"${custom.orders.topic_name}")
    private lateinit var topicName: String

    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    private val broker1 = KafkaReplicationTestContainers.broker1
    private val broker2 = KafkaReplicationTestContainers.broker2
    private val broker3 = KafkaReplicationTestContainers.broker3

    private val brokersByNodeId = mapOf(1 to broker1, 2 to broker2, 3 to broker3)

    private val adminClient by lazy { AdminClient.create(kafkaAdmin.configurationProperties) }

    private val dockerClient = DockerClientFactory.instance().client()

    private val consumer by lazy {
        val props = KafkaTestUtils.consumerProps(bootstrapServers, "replication-test", true)
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "1000"
        props[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = "2000"
        DefaultKafkaConsumerFactory<String, String>(
            props,
            StringDeserializer(),
            StringDeserializer(),
        ).createConsumer()
    }

    @BeforeEach
    fun beforeEach() {
        // check orders topic is created with replication factor 3
        // ignoreExceptions: AdminClient calls can throw if a broker is down/recovering (routed to paused node)
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            adminClient.describeTopicPartitions(topicName).first().also {
                assertThat(it.replicas()).hasSize(3)
                assertThat(it.isr()).hasSize(3)
            }
        }
    }

    @AfterEach
    fun afterEach() {
        consumer.close()
        // always resume all nodes in case a test fails midpause
        // we don't know which 1 got paused since it's decided dynamically based on leadership
        brokersByNodeId.values.forEach { runCatching { dockerClient.unpauseContainerCmd(it.containerId).exec() } }
    }

    @Test
    fun `publishing still succeeds and topic stays readable when one broker is down`() {
        // subscribe before pause so consumer is aware of everything
        consumer.subscribe(listOf(topicName))

        // get leader
        val leaderBefore =
            adminClient
                .describeTopicPartitions(topicName)
                .first()
                .leader()
                .id()
        val leaderContainer = brokersByNodeId.getValue(leaderBefore)

        // pause leader
        dockerClient.pauseContainerCmd(leaderContainer.containerId).exec()

        // wait for all partitions to evict paused broker from ISR
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            adminClient
                .describeTopicPartitions(topicName)
                .forEach { assertThat(it.isr()).hasSize(2) }
        }

        // check new leader elected
        val leaderAfter =
            adminClient
                .describeTopicPartitions(topicName)
                .first()
                .leader()
                .id()
        assertThat(leaderAfter).isNotEqualTo(leaderBefore)

        // publish should succeed
        // 2 surviving replicas are enough to satisfy acks=all with min.insync.replicas=2
        val event =
            OrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(Item(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
                totalAmountCents = 100,
            )
        val outbox = event.toOrderOutbox(objectMapper, topicName)

        // FIXME: this is failing randomly
        service.publish(listOf(outbox)).also {
            assertThat(it).hasSize(1)
            assertThat(it[0].publishedAt).isNotNull
            assertThat(it[0].attempts).isZero
        }

        // check event is retrievable
        KafkaTestUtils.getSingleRecord(consumer, topicName, Duration.ofSeconds(30)).also {
            assertThat(parseJson<OrderPlacedEvent>(it.value(), objectMapper)).isEqualTo(event)
        }

        // resume paused and check ISR recovered
        dockerClient.unpauseContainerCmd(leaderContainer.containerId).exec()
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(adminClient.describeTopicPartitions(topicName).first().isr()).hasSize(3)
        }
    }
}
