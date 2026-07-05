package com.hamza.kafka.order

import com.hamza.kafka.commons.IDrainBackOff
import com.hamza.kafka.commons.TSIDGenerator
import jakarta.validation.ConstraintViolationException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean

@DataJpaTest
@Import(PgTestContainer::class)
class DeadLetterRepositoryTest {
    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Autowired
    private lateinit var repo: DeadLetterRepository

    @MockitoBean
    private lateinit var backoff: IDrainBackOff

    private val orders =
        listOf(
            Outbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event1",
                topic = "topic",
                payload = "{}",
            ),
            Outbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event2",
                topic = "topic",
                payload = "{}",
                lastError = "error1",
            ),
        )

    @BeforeEach
    fun beforeEach() {
        whenever(backoff.isActive()).thenReturn(true)
        outboxRepo.saveAll(orders)
    }

    @Test
    fun findAll() {
        repo.findAll().also {
            assertThat(it).hasSize(1)
            assertThat(it.first().id).isEqualTo(orders.last().id)
        }
    }

    @Test
    fun `outbox row not respecting lastError validation pattern should be rejected with an error`() {
        val invalid =
            Outbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event3",
                topic = "topic",
                payload = "{}",
                lastError = " ",
            )
        assertThrows<ConstraintViolationException> { outboxRepo.saveAndFlush(invalid) }
    }
}
