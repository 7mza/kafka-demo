package com.hamza.kafka.order

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junitpioneer.jupiter.RetryingTest
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.testcontainers.DockerClientFactory
import java.time.Duration
import java.util.UUID

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.min_insync_replicas=2",
        "custom.partitions=3",
        "custom.replication_factor=3",
        "custom.topic_name=infra.test.topic",
        "spring.kafka.producer.properties.request.timeout.ms=5000",
    ],
)
@Import(PgTestContainer::class, KafkaReplicationTestContainers::class)
class KafkaInfraTest {
    @Autowired
    private lateinit var service: IPublishService

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    @Value($$"${custom.partitions}")
    private val partitions: Int = 0

    @Value($$"${custom.replication_factor}")
    private val replicas: Int = 0

    @Value($$"${spring.kafka.producer.properties.schema.registry.url}")
    private lateinit var schemaRegistryUrl: String

    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    private val broker1 = KafkaReplicationTestContainers.broker1
    private val broker2 = KafkaReplicationTestContainers.broker2
    private val broker3 = KafkaReplicationTestContainers.broker3
    private val brokersByNodeId = mapOf(1 to broker1, 2 to broker2, 3 to broker3)

    private val dockerClient = DockerClientFactory.instance().client()

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin
    private val adminClient by lazy { AdminClient.create(kafkaAdmin.configurationProperties) }

    private val consumer by lazy {
        KafkaTestUtils
            .consumerProps(bootstrapServers, UUID.randomUUID().toString(), true)
            .apply {
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            }.let { DefaultKafkaConsumerFactory<String, OrderPlacedEvent>(it).createConsumer() }
    }

    @BeforeEach
    fun beforeEach() {
        consumer.assertSubscription(topicName)
        adminClient.assertNodes(topicName = topicName, partitions = partitions, replicas = replicas, isr = replicas)
        service.warmupSchemaRegistry(topicName)
    }

    @AfterEach
    fun afterEach() {
        consumer.close()
        // always resume all nodes in case a test fails midpause
        // we don't know which 1 got paused since it's decided dynamically based on leadership
        brokersByNodeId.values.forEach { runCatching { dockerClient.unpauseContainerCmd(it.containerId).exec() } }
    }

    @Test
    fun `partition - same orderId = same partition`() {
        // gen many events with same orderId
        val outboxes =
            (1..15).map {
                createOrderPlacedEvent(
                    orderId = "order_2203",
                    customerId = "user_220$it",
                    items = listOf(createEventItem(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                ).toOutbox(topicName)
            }

        // publish events to kafka
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(outboxes).also { assertThat(it.publishedCount).isEqualTo(outboxes.size) }
        }

        // check events landed in same partition
        KafkaTestUtils
            .getRecords(consumer, Duration.ofSeconds(30))
            .records(topicName)
            .filter { it.value().orderId != warmupEvent.orderId }
            .map { it.partition() }
            .toSet()
            .also { assertThat(it).hasSize(1) }
    }

    // N events with different keys can still land in same partition
    @RetryingTest(maxAttempts = 3, suspendForMs = 2000)
    fun `partition - different orderId = spread across more than 1 partition`() {
        // gen many events with different orderId
        val outboxes =
            (1..15).map {
                createOrderPlacedEvent(
                    orderId = "order_20$it",
                    customerId = "user_220$it",
                    items = listOf(createEventItem(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                ).toOutbox(topicName)
            }

        // publish events to kafka
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(outboxes).also { assertThat(it.publishedCount).isEqualTo(outboxes.size) }
        }

        // check events spread across more than 1 partition
        KafkaTestUtils
            .getRecords(consumer, Duration.ofSeconds(30))
            .records(topicName)
            .filter { it.value().orderId != warmupEvent.orderId }
            .map { it.partition() }
            .toSet()
            .also { assertThat(it.size).isGreaterThan(1) }
    }

    @Test
    fun `replication - publishing still succeeds and topic stays readable when one broker is down`() {
        // get leader
        val leaderIdBefore = adminClient.getPartitionLeader(topicName = topicName, partition = 0)!!.id()
        val leaderContainer = brokersByNodeId.getValue(leaderIdBefore)

        // pause leader
        dockerClient.pauseContainerCmd(leaderContainer.containerId).exec()

        // wait for all partitions to evict paused broker from ISR
        adminClient.assertNodes(topicName = topicName, partitions = partitions, replicas = replicas, isr = replicas - 1)

        // check new leader elected
        adminClient
            .getPartitionLeader(topicName = topicName, partition = 0)!!
            .id()
            .also { assertThat(it).isNotEqualTo(leaderIdBefore) }

        // publish should succeed
        // 2 surviving replicas are enough to satisfy acks=all with min.insync.replicas=2
        val event =
            createOrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
            )
        val outbox = event.toOutbox(topicName)

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(listOf(outbox)).also { assertThat(it.publishedCount).isOne }
        }

        // check event is retrievable
        KafkaTestUtils
            .getRecords(consumer, Duration.ofSeconds(30))
            .records(topicName)
            .filter { it.value().orderId != warmupEvent.orderId }
            .also {
                assertThat(it).hasSize(1)
                it.first().also { sent ->
                    assertThat(sent).isNotNull
                    assertThat(sent!!.key()).isEqualTo(outbox.orderId)
                    assertThat(sent.value()).isEqualTo(event)
                }
            }

        // resume paused and check ISR recovered
        dockerClient.unpauseContainerCmd(leaderContainer.containerId).exec()
        adminClient.assertNodes(topicName = topicName, partitions = partitions, replicas = replicas, isr = replicas)
    }
}
