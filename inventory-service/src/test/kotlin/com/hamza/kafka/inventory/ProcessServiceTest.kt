package com.hamza.kafka.inventory

import com.hamza.kafka.commons.TSIDGenerator
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class ProcessServiceTest {
    @Autowired
    private lateinit var repo: InboxRepository

    @MockitoSpyBean
    private lateinit var processService: IProcessService<Inbox>

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
        )

    @BeforeEach
    fun beforeEach() {
        verify(processService, never()).process()
    }

    @AfterEach
    fun afterEach() {
        repo.deleteAll()
    }

    @Test
    fun `any change to inbox db, should trigger ProcessService and mark inbox messages as processed`() {
        repo.saveAll(orders)
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(processService, atLeastOnce()).process()
        }
        repo.findAll().also { response ->
            assertThat(response.all { it.processedAt != null }).isTrue
            assertThat(response.all { it.status != null }).isTrue
        }
    }
}
