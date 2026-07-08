package com.hamza.kafka.order

import com.hamza.kafka.commons.Status
import io.swagger.v3.oas.annotations.media.Schema
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.Positive
import jakarta.validation.constraints.Size

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
    fun toEntity() = Item(sku = this.sku, quantity = this.quantity, unitPriceCents = this.unitPriceCents)
}

data class OrderGetDto(
    val id: String,
    val customerId: String,
    val createdAt: String,
    val items: List<ItemDto>,
    val status: Status,
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
    fun toEntity() = Order(customerId = this.customerId, items = this.items.map { it.toEntity() })
}
