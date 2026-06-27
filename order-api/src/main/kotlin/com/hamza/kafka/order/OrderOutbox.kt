package com.hamza.kafka.order

import com.hamza.kafka.commons.BaseEntity
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import org.hibernate.annotations.ColumnDefault
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import java.time.Instant

@Entity
@Table(name = "orders_outbox")
class OrderOutbox(
    override var id: String,
    //
    @field:Column(nullable = false, length = 13)
    @field:NotBlank
    @field:Size(min = 13, max = 13)
    var orderId: String,
    //
    @field:Column(nullable = false, length = 100)
    @field:NotBlank
    @field:Size(max = 100)
    var eventType: String,
    //
    @field:Column(nullable = false, length = 100)
    @field:NotBlank
    @field:Size(max = 100)
    var topic: String,
    //
    @field:Column(nullable = false, columnDefinition = "jsonb")
    @field:JdbcTypeCode(SqlTypes.JSON)
    @field:NotBlank
    var payload: String,
    //
    var publishedAt: Instant? = null,
    //
    @field:Column(nullable = false)
    @field:ColumnDefault("0")
    @field:Min(0)
    var attempts: Int = 0,
    //
    @field:Column(columnDefinition = "text")
    @field:Pattern(regexp = ".*\\S.*", message = "must not be blank")
    var lastError: String? = null,
) : BaseEntity(id) {
    override fun equals(other: Any?) = this === other || (other is OrderOutbox && this.id == other.id)

    override fun hashCode() = this.id.hashCode()

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

interface IOrderOutboxRepository : JpaRepository<OrderOutbox, String> {
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
    ): List<OrderOutbox>

    fun findByOrderId(orderId: String): OrderOutbox?
}
