package com.hamza.kafka.commons

import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class BaseEvent(
    val eventId: String = TSIDGenerator.next(),
    val eventType: String,
    val occurredAt: String = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
)
