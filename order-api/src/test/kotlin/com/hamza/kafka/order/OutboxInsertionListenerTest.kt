package com.hamza.kafka.order

import eu.rekawek.toxiproxy.Proxy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.batch_size=0",
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.liquibase.enabled=true",
    ],
)
@Import(ProxiedPgTestContainer::class)
class OutboxInsertionListenerTest {
    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Autowired
    private lateinit var persistenceService: IPersistenceService

    @MockitoSpyBean
    private lateinit var trigger: IDrainTrigger

    @Autowired
    private lateinit var proxy: Proxy

    private val request =
        OrderPostDto(
            customerId = "user-2203",
            items = listOf(ItemDto(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @BeforeEach
    fun beforeEach() {
        verify(trigger, never()).trigger()
    }

    @AfterEach
    fun afterEach() {
        orderRepo.deleteAll()
        outboxRepo.deleteAll()
    }

    @Test
    fun `trigger fires again after PG is killed and resumed`() {
        persistenceService.save(request)
        await().atMost(Duration.ofSeconds(5)).untilAsserted {
            verify(trigger, times(1)).trigger()
        }

        proxy.disable()
        Thread.sleep(Duration.ofSeconds(6).toMillis())
        proxy.enable()

        persistenceService.save(request)
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            verify(trigger, times(2)).trigger()
        }
    }
}
