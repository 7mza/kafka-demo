package com.hamza.kafka.order

import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.ProxiedPgTestContainer
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
    private lateinit var repo: OutboxRepository

    @Autowired
    private lateinit var proxy: Proxy

    @MockitoSpyBean
    private lateinit var trigger: ITrigger

    @MockitoBean
    private lateinit var service: IPublishService<Outbox>

    private val outbox =
        Outbox(orderId = TSIDGenerator.next(), eventType = "eventType", topic = "topic", payload = "{}")

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
        repo.save(outbox)
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(trigger, times(1)).trigger()
        }

        proxy.disable()
        Thread.sleep(Duration.ofSeconds(6).toMillis())
        proxy.enable()

        repo.save(outbox.apply { id = TSIDGenerator.next() }) // FIXME: why was this not throwing before apply
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            verify(trigger, times(2)).trigger()
        }
    }
}
