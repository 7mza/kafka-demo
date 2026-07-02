package com.hamza.kafka.commons

import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.Repository
import java.time.Instant

interface DeadLetterProjection {
    val id: String
    val orderId: String
    val eventType: String
    val topic: String
    val payload: String
    val attempts: Int
    val lastError: String
    val createdAt: Instant
    val lastErrorAt: Instant

    fun toDto() =
        DeadLetterDto(
            id = this.id,
            orderId = this.orderId,
            eventType = this.eventType,
            topic = this.topic,
            payload = this.payload,
            attempts = this.attempts,
            lastError = this.lastError,
            createdAt = this.createdAt,
            lastErrorAt = this.lastErrorAt,
        )
}

interface BaseDeadLetterRepository<T : BaseOutbox> : Repository<T, String> {
    @Query(value = """select * from dead_letters order by "lastErrorAt" desc""", nativeQuery = true)
    fun findAll(): List<DeadLetterProjection>
}
