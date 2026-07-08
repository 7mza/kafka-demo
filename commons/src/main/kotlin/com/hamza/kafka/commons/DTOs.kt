package com.hamza.kafka.commons

import com.hamza.commons.OrderStatus
import java.time.Instant

data class DeadLetterDto(
    val id: String,
    val orderId: String,
    val eventType: String,
    val topic: String,
    val payload: String,
    val attempts: Int,
    val lastError: String,
    val createdAt: Instant,
    val lastErrorAt: Instant,
)

data class DeadLettersDto(
    val results: List<DeadLetterDto>,
)

enum class Status {
    PENDING,
    ACCEPTED,
    REJECTED,
}

fun OrderStatus.toDto() =
    when (this) {
        OrderStatus.ACCEPTED -> Status.ACCEPTED
        OrderStatus.REJECTED -> Status.REJECTED
    }
