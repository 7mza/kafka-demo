package com.hamza.kafka.order

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.verify
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["custom.poll_delay=PT1S", "custom.poll_initial_delay=PT0S"],
)
@Import(PgTestContainer::class)
class PollingServiceTest {
    @MockitoSpyBean
    private lateinit var service: IPollingService

    @MockitoSpyBean
    private lateinit var trigger: IDrainTrigger

    @Test
    fun `@Scheduled should trigger poll automatically`() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            verify(service, atLeastOnce()).poll()
            verify(trigger, atLeastOnce()).trigger()
        }
    }
}
