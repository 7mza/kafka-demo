package com.hamza.kafka.order

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

interface IDrainBackOff {
    fun isActive(): Boolean

    fun observe(result: PublishResult)
}

@Service
class DrainBackOff : IDrainBackOff {
    private companion object {
        const val COOLDOWN_STEP_MS = 10_000L
        const val MAX_COOLDOWN_MS = 60_000L
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val cooldownUntil = AtomicReference(Instant.MIN)
    private val consecutiveNoProgress = AtomicInteger(0)

    override fun isActive() = Instant.now().isBefore(cooldownUntil.get())

    override fun observe(result: PublishResult) {
        if (this.isProgressing(result)) {
            consecutiveNoProgress.set(0)
        } else if (result.recoverableErrorsCount > 0) {
            val n = consecutiveNoProgress.incrementAndGet()
            val coolDownMs = Duration.ofMillis((n * COOLDOWN_STEP_MS).coerceAtMost(MAX_COOLDOWN_MS))
            val until = Instant.now().plus(coolDownMs)
            cooldownUntil.set(until)
            logger.warn(
                "{} consecutive drain cycles with no progress, backing off for {}s",
                n,
                coolDownMs.toSeconds(),
            )
        }
    }

    private fun isProgressing(result: PublishResult) = result.publishedCount + result.deadLettersCount > 0
}
