package com.hamza.kafka.order

import com.hamza.kafka.commons.BaseEvent
import com.hamza.kafka.commons.writeJson
import tools.jackson.databind.ObjectMapper

data class OrderPlacedEvent(
    val orderId: String,
    val customerId: String,
    val items: List<Item>,
    val totalAmountCents: Int,
) : BaseEvent(eventType = "order.placed") {
    fun toOrderOutbox(
        objectMapper: ObjectMapper,
        topicName: String,
    ) = OrderOutbox(
        id = this.eventId,
        orderId = this.orderId,
        eventType = this.eventType,
        topic = topicName,
        payload = writeJson(this, objectMapper),
    )
}
