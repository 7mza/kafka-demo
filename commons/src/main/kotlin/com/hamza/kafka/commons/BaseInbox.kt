package com.hamza.kafka.commons

import jakarta.persistence.Column
import jakarta.persistence.MappedSuperclass
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.time.Instant

@MappedSuperclass
abstract class BaseInbox(
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
    @field:Column(nullable = false, columnDefinition = "jsonb")
    @field:JdbcTypeCode(SqlTypes.JSON)
    @field:NotBlank
    var payload: String,
    //
    var processedAt: Instant? = null,
) : BaseEntity()
