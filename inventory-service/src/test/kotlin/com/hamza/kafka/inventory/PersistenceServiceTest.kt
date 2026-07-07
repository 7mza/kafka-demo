package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.TSIDGenerator
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import com.hamza.kafka.commons.fromJson
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean
import kotlin.test.Test

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class PersistenceServiceTest {
    @Autowired
    private lateinit var service: IPersistenceService<OrderPlacedEvent>

    @Autowired
    private lateinit var repo: InboxRepository

    @MockitoBean
    private lateinit var trigger: ITrigger

    private val event =
        createOrderPlacedEvent(
            orderId = TSIDGenerator.next(),
            customerId = "user_2203",
            items = listOf(createEventItem(sku = "sku-01", quantity = 1, unitPriceCents = 100)),
        )

    private val inbox = event.toInbox()

    @AfterEach
    fun afterEach() {
        repo.deleteAll()
    }

    @Test
    fun `new event should be persisted as inbox`() {
        assertThat(repo.findById(inbox.id)).isNotPresent
        service.consume(event).also {
            assertThat(it.orderId).isEqualTo(event.orderId)
            assertThat(it.eventType).isEqualTo(event.schema.name)
            assertThat(fromJson<OrderPlacedEvent>(it.payload)).isEqualTo(event)
            assertThat(it.processedAt).isNull()
            assertThat(it.status).isNull()
        }
        assertThat(repo.findById(inbox.id)).isPresent
    }

    @Test
    fun `existing event should not be persisted as inbox`() {
        repo.save(inbox)
        service.consume(event).also {
            assertThat(it.orderId).isEqualTo(event.orderId)
            assertThat(it.eventType).isEqualTo(event.schema.name)
            assertThat(fromJson<OrderPlacedEvent>(it.payload)).isEqualTo(event)
            assertThat(it.processedAt).isNull()
            assertThat(it.status).isNull()
        }
        assertThat(repo.count()).isOne
    }
}
