package com.hamza.kafka.order

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.KafkaReplicationTestContainers
import com.hamza.kafka.commons.PgTestContainer
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
        "custom.min_insync_replicas=3",
        "custom.partitions=3",
        "custom.replication_factor=3",
        "custom.topics.placed=isr.test.topic",
    ],
)
@Import(PgTestContainer::class, KafkaReplicationTestContainers::class)
class KafkaInfraMinIsrTest {
    @Autowired
    private lateinit var service: IPublishService<Outbox>

    @Value($$"${custom.topics.placed}")
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
        brokersByNodeId.values.forEach { runCatching { dockerClient.unpauseContainerCmd(it.containerId).exec() } }
    }

    @Test
    fun `no write possible when ISR drops below threshold`() {
        // pausing any 1 node drop ISR to 2 which is below min.insync.replicas=3
        dockerClient.pauseContainerCmd(broker3.containerId).exec()

        // wait for all partitions to evict paused broker from ISR
        adminClient.assertNodes(topicName = topicName, partitions = partitions, replicas = replicas, isr = replicas - 1)

        val outbox =
            createOrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
            ).toOutbox(topicName)

        // broker reject with NotEnoughReplicasException (transient failure)
        // publish should return empty (nothing succeeded) and attempts must not be inc
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(listOf(outbox)).also { assertThat(it.publishedCount).isZero }
        }
        assertThat(outbox.attempts).isZero
        assertThat(outbox.publishedAt).isNull()

        // resume paused and check ISR recovered
        dockerClient.unpauseContainerCmd(broker3.containerId).exec()
        adminClient.assertNodes(topicName = topicName, partitions = partitions, replicas = replicas, isr = replicas)
    }
}
