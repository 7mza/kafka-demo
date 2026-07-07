package com.hamza.kafka.inventory

import com.hamza.commons.OrderDecidedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.BaseOutbox
import com.hamza.kafka.commons.toJson
import jakarta.persistence.Cacheable
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.springframework.data.jpa.repository.JpaRepository

fun OrderStatus.toTopic(): String =
    when (this) {
        OrderStatus.ACCEPTED -> "orders.accepted"
        OrderStatus.REJECTED -> "orders.rejected"
    }

fun OrderDecidedEvent.toOutbox() =
    Outbox(
        orderId = this.order.orderId,
        eventType = this.schema.name,
        topic = this.status.toTopic(),
        payload = this.toJson(),
    )

@Entity
@Table(name = "orders_outbox")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "orders_outbox")
class Outbox(
    orderId: String,
    eventType: String,
    topic: String,
    payload: String,
) : BaseOutbox(
        orderId = orderId,
        eventType = eventType,
        topic = topic,
        payload = payload,
    )

interface OutboxRepository : JpaRepository<Outbox, String> {
    fun findByOrderId(orderId: String): Outbox?
}
