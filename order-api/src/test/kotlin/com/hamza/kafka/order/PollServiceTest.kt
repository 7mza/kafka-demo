package com.hamza.kafka.order

import com.hamza.kafka.commons.IPollService
import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.PgTestContainer
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
class PollServiceTest {
    @MockitoSpyBean
    private lateinit var service: IPollService

    @MockitoSpyBean
    private lateinit var trigger: ITrigger

    @Test
    fun `@Scheduled should trigger poll automatically`() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(service, atLeastOnce()).poll()
            verify(trigger, atLeastOnce()).trigger()
        }
    }
}
