package com.hamza.kafka.order

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.verify
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.util.concurrent.TimeUnit

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["custom.orders.poll_delay=PT1S", "custom.orders.poll_initial_delay=PT0S"],
)
@Import(PgTestContainer::class)
class PollingServiceScheduledTest {
    @MockitoSpyBean
    private lateinit var service: IPollingService

    @Test
    fun `@Scheduled should trigger poll automatically`() {
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted { verify(service, atLeastOnce()).poll() }
    }
}
