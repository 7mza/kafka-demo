package com.hamza.kafka.order

import com.hamza.kafka.commons.IDrainBackOff
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/*
 * CountDownLatch(1) : gate with 1 lock on it
 * countDown(): remove 1 locks, at 0 gate opens
 * await() : stand at gate until it opens
 * await(N, SECONDS) : stand at gate for max Ns, return true if gate opened or false if timeout
 * join() : wait for thread to finish
 */
class DrainTriggerTest {
    @Test
    fun `while drain service is in progress, any new concurent should drop`() {
        val service = mock<IDrainService>()
        val backOff = mock<IDrainBackOff>()
        whenever(backOff.isActive()).thenReturn(false)

        val enter = CountDownLatch(1)
        val exit = CountDownLatch(1)
        val calls = AtomicInteger(0)

        whenever(service.drain()).thenAnswer {
            calls.incrementAndGet()
            enter.countDown()
            exit.await()
            null
        }

        val trigger = DrainTrigger(service, backOff)

        val a = Thread.ofVirtual().start { trigger.trigger() }
        enter.await(2, TimeUnit.SECONDS).let { assertThat(it).isTrue }
        assertThat(calls.get()).isEqualTo(1)
        verify(service, times(1)).drain()

        val b = Thread.ofVirtual().start { trigger.trigger() }
        b.join(2000)
        assertThat(b.isAlive).isFalse
        assertThat(calls.get()).isEqualTo(1)
        verify(service, times(1)).drain()

        exit.countDown()
        a.join(2000)
        assertThat(b.isAlive).isFalse
    }
}
