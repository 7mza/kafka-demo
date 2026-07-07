package com.hamza.kafka.order

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.BaseEntity
import com.hamza.kafka.commons.createOrderPlacedEvent
import jakarta.persistence.Cacheable
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.Positive
import jakarta.validation.constraints.Size
import org.hibernate.annotations.Cache
import org.hibernate.annotations.CacheConcurrencyStrategy
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import java.time.temporal.ChronoUnit

data class Item(
    @field:NotBlank
    @field:Size(max = 100)
    var sku: String,
    //
    @field:Min(1)
    var quantity: Int,
    //
    @field:Positive
    var unitPriceCents: Int,
) {
    fun toEventItem(): com.hamza.commons.Item =
        com.hamza.commons.Item
            .newBuilder()
            .setSku(sku)
            .setQuantity(quantity)
            .setUnitPriceCents(unitPriceCents)
            .build()

    fun toDto() = ItemDto(sku = this.sku, quantity = this.quantity, unitPriceCents = this.unitPriceCents)
}

@Entity
@Table(name = "orders")
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "orders")
class Order(
    @field:Column(nullable = false, length = 13)
    @field:NotBlank
    @field:Size(max = 13)
    var customerId: String,
    //
    @field:Column(nullable = false, columnDefinition = "jsonb")
    @field:JdbcTypeCode(SqlTypes.JSON)
    @field:Valid // FIXME: deprecated
    @field:NotEmpty
    var items: List<@Valid Item>,
) : BaseEntity() {
    fun toOrderPlacedEvent(): OrderPlacedEvent =
        createOrderPlacedEvent(
            orderId = this.id,
            customerId = this.customerId,
            items = this.items.map { it.toEventItem() },
        )

    fun toDto() =
        OrderGetDto(
            id = this.id,
            customerId = this.customerId,
            createdAt = this.createdAt.truncatedTo(ChronoUnit.SECONDS).toString(),
            items = this.items.map { it.toDto() },
        )
}

interface OrderRepository : JpaRepository<Order, String>
