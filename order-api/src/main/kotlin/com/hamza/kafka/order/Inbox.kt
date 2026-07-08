package com.hamza.kafka.order

import com.hamza.commons.OrderDecidedEvent
import com.hamza.kafka.commons.BaseInbox
import com.hamza.kafka.commons.toJson
import jakarta.persistence.Cacheable
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.springframework.data.jpa.repository.JpaRepository
import java.time.Instant

fun OrderDecidedEvent.toInbox() =
    Inbox(
        orderId = this.order.orderId,
        eventType = this.schema.name,
        payload = this.toJson(),
    ).apply { id = eventId }

@Entity
@Table(name = "orders_inbox")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "orders_inbox")
class Inbox(
    orderId: String,
    eventType: String,
    payload: String,
) : BaseInbox(
        orderId = orderId,
        eventType = eventType,
        payload = payload,
        processedAt = Instant.now(),
    )

interface InboxRepository : JpaRepository<Inbox, String>
