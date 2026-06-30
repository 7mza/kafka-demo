package com.hamza.kafka.order

import com.hamza.kafka.commons.TSIDGenerator
import jakarta.validation.ConstraintViolationException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.context.annotation.Import

@DataJpaTest(properties = ["spring.jpa.hibernate.ddl-auto=validate", "spring.liquibase.enabled=true"])
@Import(PgTestContainer::class)
class DeadLetterRepositoryTest {
    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Autowired
    private lateinit var repo: DeadLetterRepository

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
        assertThrows<ConstraintViolationException> {
            outboxRepo.saveAndFlush(orders.last().apply { lastError = " " })
        }
    }
}
