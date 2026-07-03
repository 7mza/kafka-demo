package com.hamza.kafka.order

import com.hamza.kafka.avro.OrderPlacedEvent
import com.hamza.kafka.commons.ResourceNotFoundException
import com.hamza.kafka.commons.TSIDGenerator
import com.hamza.kafka.commons.fromJson
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Instant
import java.time.temporal.ChronoUnit

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["spring.jpa.hibernate.ddl-auto=validate", "spring.liquibase.enabled=true"],
)
@Import(PgTestContainer::class)
class PersistenceServiceTest {
    @Autowired
    private lateinit var service: IPersistenceService

    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    private val request =
        OrderPostDto(
            customerId = "user-2203",
            items = listOf(ItemDto(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @AfterEach
    fun afterEach() {
        orderRepo.deleteAll()
        outboxRepo.deleteAll()
    }

    @Test
    fun `receiving a post order should create an entry in order db + create an event + create an entry in outbox db`() {
        val now = Instant.now()
        val offset = within(1, ChronoUnit.SECONDS)
        val sku = request.items.first().sku
        val qte = request.items.first().quantity
        val upc = request.items.first().unitPriceCents

        val order =
            service.save(request).also {
                assertThat(it.id).hasSize(13)
                assertThat(it.customerId).isEqualTo(request.customerId)
                assertThat(it.items).hasSize(1)
                assertThat(it.createdAt).isCloseTo(now, offset)
                assertThat(it.updatedAt).isCloseTo(now, offset)
                assertThat(it.version).isZero
                it.items.first().also { item ->
                    assertThat(item.sku).isEqualTo(sku)
                    assertThat(item.quantity).isEqualTo(qte)
                    assertThat(item.unitPriceCents).isEqualTo(upc)
                }
            }

        val type = order.toOrderPlacedEvent().schema.name

        outboxRepo.findByOrderId(order.id).also {
            assertThat(it).isNotNull
            assertThat(it!!.id).hasSize(13)
            assertThat(it.orderId).isEqualTo(order.id)
            assertThat(it.eventType).isEqualTo(type)
            assertThat(it.topic).isEqualTo(topicName)
            assertThat(it.publishedAt).isNull()
            assertThat(it.attempts).isZero
            assertThat(it.createdAt).isCloseTo(now, offset)
            assertThat(it.updatedAt).isCloseTo(now, offset)
            assertThat(it.version).isZero

            fromJson<OrderPlacedEvent>(it.payload).also { event ->
                assertThat(event.eventId).hasSize(13)
                assertThat(event.schema.name).isEqualTo(type)
                assertThat(event.occurredAt).isCloseTo(now, offset)
                assertThat(event.orderId).isEqualTo(order.id)
                assertThat(event.customerId).isEqualTo(order.customerId)
                assertThat(event.items).hasSize(1)
                assertThat(event.totalAmountCents)
                    .isEqualTo(order.items.sumOf { item -> item.quantity * item.unitPriceCents })
                event.items.first().also { item ->
                    assertThat(item.sku).isEqualTo(sku)
                    assertThat(item.quantity).isEqualTo(qte)
                    assertThat(item.unitPriceCents).isEqualTo(upc)
                }
            }
        }
    }

    @Test
    fun getOrderById() {
        val saved = service.save(request)
        service.getOrderById(saved.id).also {
            assertThat(it).isEqualTo(saved)
        }
    }

    @Test
    fun getOrderByWrongId() {
        assertThrows<ResourceNotFoundException> {
            service.getOrderById("")
        }
    }

    @Test
    fun getOutboxByOrderId() {
        val order = service.save(request)
        val outbox = outboxRepo.findByOrderId(order.id)
        service.getOutboxByOrderId(order.id).also {
            assertThat(it).isEqualTo(outbox)
        }
    }

    @Test
    fun getOutboxByWrongOrderId() {
        assertThrows<ResourceNotFoundException> {
            service.getOutboxByOrderId("")
        }
    }

    @Test
    fun getDeadLetters() {
        val orders =
            listOf(
                Outbox(
                    id = TSIDGenerator.next(),
                    orderId = TSIDGenerator.next(),
                    eventType = "event1",
                    topic = "topic",
                    payload = "{}",
                    publishedAt = Instant.now(),
                ),
                Outbox(
                    id = TSIDGenerator.next(),
                    orderId = TSIDGenerator.next(),
                    eventType = "event2",
                    topic = "topic",
                    payload = "{}",
                    attempts = 10,
                    lastError = "toto",
                ),
            )

        outboxRepo.saveAll(orders)

        service.getDeadLetters().also {
            assertThat(it).hasSize(1)
            assertThat(it.first().id).isEqualTo(orders.last().id)
        }
    }
}
