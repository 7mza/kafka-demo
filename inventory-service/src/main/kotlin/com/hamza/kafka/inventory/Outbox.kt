package com.hamza.kafka.inventory

import com.hamza.commons.OrderDecidedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.BaseOutbox
import com.hamza.kafka.commons.toJson
import jakarta.persistence.Cacheable
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository

fun OrderStatus.toTopic(
    acceptedTopic: String,
    rejectedTopic: String,
): String =
    when (this) {
        OrderStatus.ACCEPTED -> acceptedTopic
        OrderStatus.REJECTED -> rejectedTopic
    }

fun OrderDecidedEvent.toOutbox(
    acceptedTopic: String,
    rejectedTopic: String,
    serializer: IAvroSerializer<OrderDecidedEvent>,
): Outbox =
    this.status.toTopic(acceptedTopic = acceptedTopic, rejectedTopic = rejectedTopic).let {
        Outbox(
            orderId = this.order.orderId,
            eventType = this.schema.name,
            topic = it,
            payload = this.toJson(),
            avroPayload = serializer.toAvroBytes(topic = it, event = this),
        )
    }

@Entity
@Table(name = "orders_outbox")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "orders_outbox")
class Outbox(
    orderId: String,
    eventType: String,
    topic: String,
    payload: String,
    // mirror payload (json) as avro bytes for Debezium FIXME: find a better way
    @field:Column(nullable = false)
    @field:JdbcTypeCode(SqlTypes.VARBINARY)
    var avroPayload: ByteArray,
) : BaseOutbox(
        orderId = orderId,
        eventType = eventType,
        topic = topic,
        payload = payload,
    )

interface OutboxRepository : JpaRepository<Outbox, String> {
    fun findByOrderId(orderId: String): Outbox?
}
