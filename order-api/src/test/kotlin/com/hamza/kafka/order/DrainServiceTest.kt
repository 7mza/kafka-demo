package com.hamza.kafka.order

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@Disabled // already tested in E2E
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.batch_size=0", // just verifying triggers, don't care about content
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.liquibase.enabled=true",
    ],
)
@Import(PgTestContainer::class)
class DrainServiceTest {
    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Autowired
    private lateinit var persistenceService: IPersistenceService

    @MockitoSpyBean
    private lateinit var trigger: IDrainTrigger

    @MockitoSpyBean
    private lateinit var drainService: IDrainService

    private val request =
        OrderPostDto(
            customerId = "user-2203",
            items = listOf(ItemDto(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @BeforeEach
    fun beforeEach() {
        // check DrainService never triggers before db changed
        verify(trigger, never()).trigger()
        verify(drainService, never()).drainOutboxes()
    }

    @AfterEach
    fun afterEach() {
        orderRepo.deleteAll()
        outboxRepo.deleteAll()
    }

    @Test
    fun `DrainService should trigger when an outbox is inserted in DB`() {
        // change DB
        persistenceService.save(request)
        // check DrainService triggers
        await().atMost(Duration.ofSeconds(5)).untilAsserted {
            verify(trigger, times(1)).trigger()
            verify(drainService, times(1)).drainOutboxes()
        }
    }
}
