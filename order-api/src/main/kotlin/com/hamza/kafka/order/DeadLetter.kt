package com.hamza.kafka.order

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
}

interface IDeadLetterOutboxRepository : Repository<OrderOutbox, String> {
    @Query(value = """select * from dead_letter_outbox order by "lastErrorAt" desc""", nativeQuery = true)
    fun findAll(): List<DeadLetterProjection>
}
