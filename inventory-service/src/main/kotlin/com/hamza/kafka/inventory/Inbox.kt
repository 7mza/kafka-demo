package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.BaseInbox
import com.hamza.kafka.commons.createOrderDecidedEvent
import com.hamza.kafka.commons.fromJson
import com.hamza.kafka.commons.toJson
import jakarta.persistence.Cacheable
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Table
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

fun OrderPlacedEvent.toInbox() =
    Inbox(
        orderId = this.orderId,
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
    //
    @field:Enumerated(EnumType.ORDINAL)
    var status: OrderStatus? = null,
) : BaseInbox(
        orderId = orderId,
        eventType = eventType,
        payload = payload,
    ) {
    fun toOrderDecidedEvent() =
        createOrderDecidedEvent(
            order = fromJson<OrderPlacedEvent>(this.payload),
            status = this.status!!, // !! on null should never happen
        )
}

interface InboxRepository : JpaRepository<Inbox, String> {
    @Query(
        value = """
            select * from orders_inbox
            where "processedAt" is null
            order by "createdAt"
            limit :batchSize
            for update skip locked
            """,
        nativeQuery = true,
    )
    fun retrieveUnprocessed(batchSize: Int): List<Inbox>

    fun findByOrderId(orderId: String): Inbox?
}
