package com.hamza.kafka.inventory

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ProcessTriggerTest {
    @Test
    fun `while trigger service is in progress, any new concurent should drop`() {
        val service = mock<IProcessService<Inbox>>()

        val enter = CountDownLatch(1)
        val exit = CountDownLatch(1)
        val calls = AtomicInteger(0)

        whenever(service.process()).thenAnswer {
            calls.incrementAndGet()
            enter.countDown()
            exit.await()
            null
        }

        val trigger = ProcessTrigger(service)

        val a = Thread.ofVirtual().start { trigger.trigger() }
        enter.await(2, TimeUnit.SECONDS).also { assertThat(it).isTrue }
        assertThat(calls.get()).isOne
        verify(service, times(1)).process()

        val b = Thread.ofVirtual().start { trigger.trigger() }
        b.join(2000)
        assertThat(b.isAlive).isFalse
        assertThat(calls.get()).isOne
        verify(service, times(1)).process()

        exit.countDown()
        a.join(2000)
        assertThat(b.isAlive).isFalse
    }
}
