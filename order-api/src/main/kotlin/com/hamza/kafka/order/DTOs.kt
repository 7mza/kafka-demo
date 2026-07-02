package com.hamza.kafka.order

import com.hamza.kafka.commons.DeadLetterProjection
import io.swagger.v3.oas.annotations.media.Schema
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.Positive
import jakarta.validation.constraints.Size
import java.time.Instant

data class ItemDto(
    @field:NotBlank
    @field:Size(max = 100)
    @field:Schema(example = "sku-01")
    val sku: String,
    //
    @field:Min(1)
    @field:Schema(example = "10")
    val quantity: Int,
    //
    @field:Positive
    @field:Schema(example = "199")
    val unitPriceCents: Int,
) {
    fun toEntity() =
        Item(
            sku = this.sku,
            quantity = this.quantity,
            unitPriceCents = this.unitPriceCents,
        )
}

data class OrderGetDto(
    val id: String,
    val customerId: String,
    val createdAt: String,
    val items: List<ItemDto>,
)

data class OrderPostDto(
    @field:NotBlank
    @field:Size(max = 100)
    @field:Schema(example = "user_2203")
    val customerId: String,
    //
    @field:Valid // FIXME: deprecated
    @field:NotEmpty
    val items: List<@Valid ItemDto>,
) {
    fun toEntity() =
        Order(
            customerId = this.customerId,
            items = this.items.map { it.toEntity() },
        )
}

@Schema(description = "outbox record for a placed order")
data class OrderOutboxDto(
    @field:Schema(description = "outbox id", example = "0qtc1hmz3p6nk")
    val id: String,
    @field:Schema(description = "id of the order this event belongs to", example = "0qsbs74grkjq2")
    val orderId: String,
    @field:Schema(description = "event type", example = "order.placed")
    val eventType: String,
    @field:Schema(description = "kafka topic the event will be published to", example = "orders")
    val topic: String,
    @field:Schema(description = "payload of the event to be sent to Kafka (raw JSON)")
    val payload: String,
    @field:Schema(description = "timestamp of successful Kafka publish (null if not published yet)")
    val publishedAt: Instant?,
    @field:Schema(
        description = "number of publish attempts (> 0 if there was an error, 10 is upper limit)",
        example = "0",
    )
    val attempts: Int,
    @field:Schema(description = "last error message from failed publish attempt (null if no failure)")
    val lastError: String?,
)

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

fun DeadLetterProjection.toDto() =
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

data class DeadLettersDto(
    val results: List<DeadLetterDto>,
)
