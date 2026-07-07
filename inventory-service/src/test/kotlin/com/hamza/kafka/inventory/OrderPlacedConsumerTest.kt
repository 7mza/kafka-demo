package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.TSIDGenerator
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import com.hamza.kafka.commons.fromJson
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class, KafkaTestContainer::class)
class OrderPlacedConsumerTest {
    @Autowired
    private lateinit var inboxRepo: InboxRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    @Value($$"${spring.kafka.consumer.group-id}")
    private lateinit var groupId: String

    @Value($$"${spring.kafka.consumer.properties.schema.registry.url}")
    private lateinit var schemaRegistryUrl: String

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Autowired
    private lateinit var kafkaAdmin: KafkaAdmin
    private val adminClient by lazy { AdminClient.create(kafkaAdmin.configurationProperties) }

    private val producer by lazy {
        KafkaTestUtils
            .producerProps(kafkaContainer.bootstrapServers)
            .apply {
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
            }.let { DefaultKafkaProducerFactory<String, OrderPlacedEvent>(it).createProducer() }
    }

    @AfterEach
    fun afterEach() {
        inboxRepo.deleteAll()
        outboxRepo.deleteAll()
        producer.close()
    }

    @Test
    fun `consume event and persist inbox row`() {
        val event =
            createOrderPlacedEvent(
                orderId = TSIDGenerator.next(),
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
            )

        producer.send(ProducerRecord(topicName, event.eventId, event)).get()

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(inboxRepo.count()).isOne
            inboxRepo.findAll().first().also {
                assertThat(it.processedAt).isNotNull
                assertThat(it.status).isNotNull
                assertThat(fromJson<OrderPlacedEvent>(it.payload)).isEqualTo(event)
            }
        }

        // check ack
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get()
                .also { future ->
                    future
                        .filterKeys { it.topic() == topicName }
                        .values
                        .sumOf { it.offset() }
                        .also { assertThat(it).isOne }
                }
        }
    }
}
