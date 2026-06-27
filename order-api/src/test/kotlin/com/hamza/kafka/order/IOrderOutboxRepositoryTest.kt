package com.hamza.kafka.order

import com.hamza.kafka.commons.TSIDGenerator
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class IOrderOutboxRepositoryTest {
    @Autowired
    private lateinit var repo: IOrderOutboxRepository

    @Autowired
    private lateinit var txManager: PlatformTransactionManager

    private val orders =
        listOf(
            OrderOutbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event1",
                topic = "topic",
                payload = "{}",
            ),
            OrderOutbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event2",
                topic = "topic",
                payload = "{}",
            ),
            OrderOutbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event3",
                topic = "topic",
                payload = "{}",
                publishedAt = Instant.now(),
            ),
            OrderOutbox(
                id = TSIDGenerator.next(),
                orderId = TSIDGenerator.next(),
                eventType = "event4",
                topic = "topic",
                payload = "{}",
                attempts = 10,
            ),
        )

    @BeforeEach
    fun beforeEach() {
        repo.saveAll(orders)
    }

    @AfterEach
    fun afterEach() {
        repo.deleteAll()
    }

    @Test
    fun `retrieveUnpublished should not return outbox messages that were already published or dead letters`() {
        repo.retrieveUnpublished(10, 10).also { response ->
            assertThat(response).hasSize(2)
            assertThat(response.all { it.publishedAt == null }).isTrue // check not published yet
            assertThat(response.all { it.attempts < 10 }).isTrue // check not a dead letter
            assertThat(response.first().createdAt).isBefore(response.last().createdAt) // check order by createdAt FIFO
        }
    }

    @Test
    fun `retrieveUnpublished should respect batchSize`() {
        repo.retrieveUnpublished(1, 10).also {
            assertThat(it).hasSize(1)
            assertThat(it.first().eventType).isEqualTo(orders.first().eventType)
        }
    }

    @Test
    fun `when multiple concurent retrieveUnpublished triggers, each picks a distinct batch via SKIP LOCKED`() {
        val lockAcquired = CountDownLatch(1)
        val secondAttemptDone = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)

        var firstBatch: List<OrderOutbox> = emptyList()
        var secondBatch: List<OrderOutbox> = emptyList()

        val first =
            pool.submit {
                TransactionTemplate(txManager).execute {
                    repo.retrieveUnpublished(1, 10).also {
                        firstBatch = it
                        assertThat(it).hasSize(1)
                        lockAcquired.countDown()
                        secondAttemptDone.await() // keep txn/lock open until 2nd caller tried
                    }
                }
            }

        val second =
            pool.submit {
                lockAcquired.await() // only query after 1st caller holds lock
                try {
                    TransactionTemplate(txManager).execute {
                        repo.retrieveUnpublished(1, 10).also {
                            secondBatch = it
                            assertThat(it).hasSize(1) // SKIP LOCKED skips the locked row and picks the next one
                        }
                    }
                } finally {
                    secondAttemptDone.countDown()
                }
            }

        first.get()
        second.get()

        assertThat(firstBatch.map { it.id }).doesNotContainAnyElementsOf(secondBatch.map { it.id })

        repo.retrieveUnpublished(10, 10).also {
            assertThat(it).hasSize(2) // check freed after release
        }
    }
}
