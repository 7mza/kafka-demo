package com.hamza.kafka.order

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

class DrainBackOffTest {
    private var now = Instant.parse("2026-07-02T00:00:00Z")

    private val clock =
        object : Clock() {
            override fun instant() = now

            override fun getZone() = ZoneOffset.UTC

            override fun withZone(zone: ZoneId?) = this
        }

    private val backOff = DrainBackOff(clock)

    private val progressing = PublishResult(publishedCount = 1)

    private val recoverable = PublishResult(recoverableErrorsCount = 1)

    @Test
    fun `progress = no backoff`() {
        backOff.observe(progressing)
        assertThat(backOff.isActive()).isFalse
    }

    @Test
    fun `1st no progress = 10s cooldown`() {
        backOff.observe(recoverable)
        assertThat(backOff.isActive()).isTrue
        now = now.plusSeconds(9)
        assertThat(backOff.isActive()).isTrue // still CD
        now = now.plusSeconds(2) // 11s
        assertThat(backOff.isActive()).isFalse // no CD
    }

    @Test
    fun `cooldown grow linear but max at 60s`() {
        repeat(7) { backOff.observe(recoverable) } // 70s
        now = now.plusSeconds(59)
        assertThat(backOff.isActive()).isTrue // still CD
        now = now.plusSeconds(2) // 61s
        assertThat(backOff.isActive()).isFalse // no CD
    }

    @Test
    fun `progress reset CD`() {
        repeat(3) { backOff.observe(recoverable) } // 30s
        backOff.observe(progressing) // reset
        backOff.observe(recoverable) // 10s
        now = now.plusSeconds(11)
        assertThat(backOff.isActive()).isFalse
    }
}

// val x = Foo() cache same
// fun x() = Foo() rebuild new
