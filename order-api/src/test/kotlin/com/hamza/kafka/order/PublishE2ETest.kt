package com.hamza.kafka.order

import com.hamza.kafka.avro.OrderPlacedEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.UUID

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["spring.jpa.hibernate.ddl-auto=validate", "spring.liquibase.enabled=true"],
)
@Import(PgTestContainer::class, KafkaTestContainer::class)
@AutoConfigureRestTestClient
class PublishE2ETest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var client: RestTestClient

    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

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
        orderRepo.deleteAll()
        outboxRepo.deleteAll()
        consumer.close()
    }

    @Test
    fun `publish orders end to end integration test`() {
        // post 3 orders
        val requestsById =
            (1..3).associate {
                val request =
                    OrderPostDto(
                        customerId = "user_220$it",
                        items = listOf(ItemDto(sku = "sku-0$it", quantity = 10 * it, unitPriceCents = 100 * it)),
                    )
                val id =
                    client
                        .post()
                        .uri("/api/order")
                        .body(request)
                        .exchange()
                        .expectStatus()
                        .isCreated
                        .expectBody<OrderGetDto>()
                        .returnResult()
                        .responseBody!!
                        .id
                id to request
            }
        val ids = requestsById.keys.toList()

        // check orders saved to DB
        requestsById.forEach { (id, request) ->
            orderRepo.findById(id).orElse(null).also {
                assertThat(it).isNotNull
                assertThat(it!!.customerId).isEqualTo(request.customerId)
                assertThat(it.items).isEqualTo(request.items.map { item -> item.toEntity() })
            }
        }

        // check orders outbox saved to DB and published without error
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            ids.forEach {
                outboxRepo.findByOrderId(it).also { outbox ->
                    assertThat(outbox).isNotNull
                    assertThat(outbox!!.publishedAt).isNotNull
                    assertThat(outbox.attempts).isZero
                }
            }
        }

        // check events published to kafka
        val expectedEvents = orderRepo.findAll().map { it.toOrderPlacedEvent() }
        KafkaTestUtils
            .getRecords(consumer, Duration.ofSeconds(30))
            .records(topicName)
            .map { it.value() }
            .filter { it.orderId != warmupEvent.orderId }
            .also {
                assertThat(it)
                    .usingRecursiveFieldByFieldElementComparator(
                        RecursiveComparisonConfiguration.builder().withIgnoredFields("eventId", "occurredAt").build(),
                    ).containsExactlyInAnyOrder(*expectedEvents.toTypedArray())
            }
    }
}
