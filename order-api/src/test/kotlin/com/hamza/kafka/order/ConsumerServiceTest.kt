package com.hamza.kafka.order

import com.hamza.commons.OrderDecidedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.IConsumerService
import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.PgTestContainer
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderDecidedEvent
import com.hamza.kafka.commons.createOrderPlacedEvent
import com.hamza.kafka.commons.fromJson
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean
import kotlin.test.Test

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class ConsumerServiceTest {
    @Autowired
    private lateinit var service: IConsumerService<OrderDecidedEvent, Inbox>

    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var inboxRepo: InboxRepository

    @MockitoBean
    private lateinit var trigger: ITrigger

    private val order =
        Order(customerId = "user_2203", items = listOf(Item(sku = "sku-01", quantity = 1, unitPriceCents = 100)))

    private val event =
        createOrderDecidedEvent(
            order =
                createOrderPlacedEvent(
                    orderId = order.id,
                    customerId = "user_2203",
                    items = listOf(createEventItem(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
                ),
            status = OrderStatus.ACCEPTED,
        )

    private val inbox = event.toInbox()

    @BeforeEach
    fun beforeEach() {
        orderRepo.save(order)
    }

    @AfterEach
    fun afterEach() {
        orderRepo.deleteAll()
        inboxRepo.deleteAll()
    }

    @Test
    fun `new event should be persisted as inbox and set it's order status`() {
        assertThat(inboxRepo.findById(inbox.id)).isNotPresent
        service.consume(event).also {
            assertThat(it.id).isEqualTo(event.eventId)
            assertThat(it.orderId).isEqualTo(event.order.orderId)
            assertThat(it.eventType).isEqualTo(event.schema.name)
            assertThat(fromJson<OrderDecidedEvent>(it.payload)).isEqualTo(event)
            assertThat(it.processedAt).isNotNull
        }
        assertThat(inboxRepo.findById(inbox.id)).isPresent

        orderRepo.findById(order.id).also {
            assertThat(it.isPresent).isTrue
            assertThat(it.get().status).isEqualTo(event.status)
        }
    }

    @Test
    fun `existing event should not be persisted as inbox`() {
        inboxRepo.save(inbox)
        service.consume(event).also {
            assertThat(it.orderId).isEqualTo(event.order.orderId)
            assertThat(it.eventType).isEqualTo(event.schema.name)
            assertThat(fromJson<OrderDecidedEvent>(it.payload)).isEqualTo(event)
            assertThat(it.processedAt).isNotNull
        }
        assertThat(inboxRepo.count()).isOne

        orderRepo.findById(order.id).also {
            assertThat(it.isPresent).isTrue
            assertThat(it.get().status).isNull()
        }
    }
}
