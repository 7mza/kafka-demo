package com.hamza.kafka.commons

import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

interface IDrainBackOff {
    fun isActive(): Boolean

    fun observe(result: KafkaPublishResult)
}

class DrainBackOff(
    private val clock: Clock = Clock.systemUTC(),
) : IDrainBackOff {
    private companion object {
        const val COOLDOWN_STEP_MS = 10_000L
        const val MAX_COOLDOWN_MS = 60_000L
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val cooldownUntil = AtomicReference(Instant.MIN)
    private val consecutiveNoProgress = AtomicInteger(0)

    // is cooldown still on
    override fun isActive() = clock.instant().isBefore(cooldownUntil.get())

    /* observe result of publish service
     * if we're not progressing (nothing published or dead-lettered)
     *  record for how many consecutive cycle
     *  then use it to create a linear cooldown
     *  this CD will prevent any drain trigger (poll, CDC, self)
     *
     *  back off on any no progress cycle not just recoverable ones
     *  (batch stuck on permanent errors < max_attempts would otherwise loop quickly until max attempts)
     *
     * if we're progressing
     *  reset
     */
    override fun observe(result: KafkaPublishResult) {
        if (result.isProgressing()) {
            consecutiveNoProgress.set(0)
        } else {
            val n = consecutiveNoProgress.incrementAndGet()
            val coolDownMs = Duration.ofMillis((n * COOLDOWN_STEP_MS).coerceAtMost(MAX_COOLDOWN_MS))
            val until = clock.instant().plus(coolDownMs)
            cooldownUntil.set(until)
            logger.warn("{} consecutive drain cycles with no progress, backing off for {}s", n, coolDownMs.toSeconds())
        }
    }
}
