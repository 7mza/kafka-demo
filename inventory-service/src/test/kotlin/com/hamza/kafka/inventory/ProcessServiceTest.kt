package com.hamza.kafka.inventory

import com.hamza.commons.OrderDecidedEvent
import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.PgTestContainer
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import com.hamza.kafka.commons.fromJson
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["spring.kafka.consumer.properties.schema.registry.url=mock://localhost:8080"],
)
@Import(PgTestContainer::class)
class ProcessServiceTest {
    @Autowired
    private lateinit var inboxRepo: InboxRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @MockitoSpyBean
    private lateinit var processService: IProcessService<Inbox>

    @Value($$"${spring.kafka.consumer.properties.schema.registry.url}")
    private lateinit var registryUrl: String

    @Value($$"${custom.topics.accepted}")
    private lateinit var acceptedTopic: String

    @Value($$"${custom.topics.rejected}")
    private lateinit var rejectedTopic: String

    // inverse of AvroSerializer.kt
    private val deserializer by lazy {
        KafkaAvroDeserializer().apply {
            configure(
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
                    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to true,
                ),
                false,
            )
        }
    }

    private val events =
        listOf(
            createOrderPlacedEvent(
                orderId = "0qsbs74grkjq2",
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
            ),
            createOrderPlacedEvent(
                orderId = "0qsbs74grkjq3",
                customerId = "user_2903",
                items = listOf(createEventItem(sku = "sku-02", quantity = 11, unitPriceCents = 200)),
            ),
        )

    private val inboxes = events.map { it.toInbox() }

    @BeforeEach
    fun beforeEach() {
        verify(processService, never()).process()
    }

    @AfterEach
    fun afterEach() {
        inboxRepo.deleteAll()
        outboxRepo.deleteAll()
    }

    @Test
    fun `any change to inbox db should trigger ProcessService`() {
        inboxRepo.saveAll(inboxes)
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(processService, atLeastOnce()).process()
        }

        // check inboxes created with related placed events
        events.forEach { placed ->
            inboxRepo.findByOrderId(placed.orderId).also { inbox ->
                assertThat(inbox).isNotNull
                assertThat(inbox!!.processedAt).isNotNull
                assertThat(inbox.status).isNotNull
                assertThat(fromJson<OrderPlacedEvent>(inbox.payload)).isEqualTo(placed)

                // check outboxes created with related decided events
                outboxRepo.findByOrderId(placed.orderId).also { outbox ->
                    assertThat(outbox).isNotNull
                    assertThat(outbox!!.eventType).isEqualTo(inbox.toOrderDecidedEvent().schema.name)
                    assertThat(outbox.topic)
                        .isEqualTo(inbox.status!!.toTopic(acceptedTopic = acceptedTopic, rejectedTopic = rejectedTopic))
                    fromJson<OrderDecidedEvent>(outbox.payload).also { payload ->
                        assertThat(payload.status).isEqualTo(inbox.status)
                        assertThat(payload.order).isEqualTo(placed)
                    }

                    // check bytea is exactly JSON payload
                    assertThat(outbox.avroPayload).isNotEmpty()
                    assertThat(outbox.avroPayload.first()).isEqualTo(0.toByte()) // avro leading byte 0x00
                    (deserializer.deserialize(outbox.topic, outbox.avroPayload) as OrderDecidedEvent).also { decoded ->
                        assertThat(decoded).isEqualTo(fromJson<OrderDecidedEvent>(outbox.payload))
                    }
                }
            }
        }
    }
}
