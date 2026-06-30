package com.hamza.kafka.order

import com.hamza.kafka.commons.BaseOutbox
import jakarta.persistence.Cacheable
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.Instant

@Entity
@Table(name = "orders_outbox")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "orders_outbox")
class Outbox(
    id: String,
    orderId: String,
    eventType: String,
    topic: String,
    payload: String,
    publishedAt: Instant? = null,
    attempts: Int = 0,
    lastError: String? = null,
) : BaseOutbox(
        id = id,
        orderId = orderId,
        eventType = eventType,
        topic = topic,
        payload = payload,
        publishedAt = publishedAt,
        attempts = attempts,
        lastError = lastError,
    ) {
    fun toDto() =
        OrderOutboxDto(
            id = this.id,
            orderId = this.orderId,
            eventType = this.eventType,
            topic = this.topic,
            payload = this.payload,
            publishedAt = this.publishedAt,
            attempts = this.attempts,
            lastError = this.lastError,
        )
}

interface OutboxRepository : JpaRepository<Outbox, String> {
    @Query(
        value = """
            select * from orders_outbox
            where "publishedAt" is null
            and attempts < :maxAttempts
            order by "createdAt"
            limit :batchSize
            for update skip locked
            """,
        nativeQuery = true,
    )
    fun retrieveUnpublished(
        batchSize: Int,
        maxAttempts: Int,
    ): List<Outbox>

    fun findByOrderId(orderId: String): Outbox?
}
