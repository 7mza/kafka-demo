package com.hamza.kafka.inventory

import com.hamza.kafka.commons.ICDCListener
import com.hamza.kafka.commons.TSIDGenerator
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class InboxRepositoryTest {
    @Autowired
    private lateinit var repo: InboxRepository

    @Autowired
    private lateinit var txManager: PlatformTransactionManager

    @MockitoBean
    private lateinit var listener: ICDCListener

    private val orders =
        listOf(
            Inbox(
                orderId = TSIDGenerator.next(),
                eventType = "event1",
                payload = "{}",
            ),
            Inbox(
                orderId = TSIDGenerator.next(),
                eventType = "event2",
                payload = "{}",
            ),
            Inbox(
                orderId = TSIDGenerator.next(),
                eventType = "event3",
                payload = "{}",
            ).apply { processedAt = Instant.now() },
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
    fun `retrieveUnprocessed should not return inbox messages that were already processed`() {
        repo.retrieveUnprocessed(10).also { response ->
            assertThat(response).hasSize(2)
            assertThat(response.all { it.processedAt == null }).isTrue // check not published yet
            assertThat(response.all { it.status == null }).isTrue
            assertThat(response.first().createdAt).isBefore(response.last().createdAt) // check order by createdAt FIFO
        }
    }

    @Test
    fun `retrieveUnprocessed should respect batchSize`() {
        repo.retrieveUnprocessed(1).also {
            assertThat(it).hasSize(1)
            assertThat(it.first().eventType).isEqualTo(orders.first().eventType)
        }
    }

    @Test
    fun `when multiple concurent retrieveUnprocessed triggers, each picks a distinct batch via SKIP LOCKED`() {
        val lockAcquired = CountDownLatch(1)
        val secondAttemptDone = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)

        var firstBatch: List<Inbox> = emptyList()
        var secondBatch: List<Inbox> = emptyList()

        val first =
            pool.submit {
                TransactionTemplate(txManager).execute {
                    repo.retrieveUnprocessed(1).also {
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
                        repo.retrieveUnprocessed(1).also {
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

        // check freed after release
        repo.retrieveUnprocessed(10).also { assertThat(it).hasSize(2) }
    }
}
