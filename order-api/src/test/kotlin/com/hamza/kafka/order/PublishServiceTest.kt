package com.hamza.kafka.order

import com.hamza.kafka.avro.OrderPlacedEvent
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
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.UUID

// FIXME: duplicate test with E2E
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class, KafkaTestContainer::class)
class PublishServiceTest {
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

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin
    private val adminClient by lazy { AdminClient.create(kafkaAdmin.configurationProperties) }

    private val consumer by lazy {
        KafkaTestUtils
            .consumerProps(kafkaContainer.bootstrapServers, UUID.randomUUID().toString(), true)
            .apply {
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000")
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
    }

    @Test
    fun `publish should send order to kafka and mark related outbox as published`() {
        // gen event + outbox
        val event =
            createOrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
            )
        val outbox = event.toOutbox(topicName)

        // publish event to kafka
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(listOf(outbox)).also { assertThat(it.publishedCount).isOne }
        }

        // check event was sent to kafka
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
    }

    @Test
    fun `publish forced dead letter`() {
        // gen event + outbox
        val event =
            createOrderPlacedEvent(
                orderId = "order_2203",
                customerId = "fail_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
            )
        val outbox = event.toOutbox(topicName)

        // publish event to kafka
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            service.publish(listOf(outbox)).also {
                assertThat(it.publishedCount).isZero
                assertThat(it.deadLettersCount).isOne
            }
        }

        // check event was not sent to kafka
        KafkaTestUtils
            .getRecords(consumer, Duration.ofSeconds(30))
            .records(topicName)
            .filter { it.value().orderId != warmupEvent.orderId }
            .also { assertThat(it).isEmpty() }
    }
}
