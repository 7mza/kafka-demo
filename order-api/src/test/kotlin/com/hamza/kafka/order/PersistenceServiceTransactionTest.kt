package com.hamza.kafka.order

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class PersistenceServiceTransactionTest {
    @Autowired
    private lateinit var service: IPersistenceService

    @Autowired
    private lateinit var orderRepo: OrderRepository

    @MockitoSpyBean
    private lateinit var outboxRepo: OutboxRepository

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
    fun `if outbox persistence fail, the whole transaction should be rolled back`() {
        doThrow(RuntimeException("error")).whenever(outboxRepo).save(any<Outbox>())
        assertThrows<RuntimeException> { service.save(request) }
        assertThat(orderRepo.count()).isZero
        assertThat(outboxRepo.count()).isZero
    }
}
