package com.hamza.kafka.inventory

import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.TSIDGenerator
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
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.time.Duration

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(ProxiedPgTestContainer::class)
class CDCListenerTest {
    @Autowired
    private lateinit var repo: InboxRepository

    @Autowired
    private lateinit var proxy: Proxy

    @MockitoSpyBean
    private lateinit var trigger: ITrigger

    @MockitoBean
    private lateinit var service: IProcessService<Inbox>

    private val inbox =
        Inbox(orderId = TSIDGenerator.next(), eventType = "eventType", payload = "{}")

    @BeforeEach
    fun beforeEach() {
        verify(trigger, never()).trigger()
    }

    @AfterEach
    fun afterEach() {
        repo.deleteAll()
    }

    @Test
    fun `trigger fires again after PG is killed and resumed`() {
        repo.save(inbox)
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(trigger, times(1)).trigger()
        }

        proxy.disable()
        Thread.sleep(Duration.ofSeconds(6).toMillis())
        proxy.enable()

        repo.save(inbox.apply { id = TSIDGenerator.next() })
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(trigger, times(2)).trigger()
        }
    }
}
