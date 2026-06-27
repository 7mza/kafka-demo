package com.hamza.kafka.commons

import jakarta.persistence.Column
import jakarta.persistence.Id
import jakarta.persistence.MappedSuperclass
import jakarta.persistence.Version
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.Size
import org.hibernate.annotations.ColumnDefault
import org.hibernate.annotations.CreationTimestamp
import org.hibernate.annotations.UpdateTimestamp
import java.time.Instant

@MappedSuperclass
abstract class BaseEntity(
    @field:Id
    @field:Column(length = 13)
    @field:Size(min = 13, max = 13)
    var id: String,
    //
    @field:Column(nullable = false)
    @field:CreationTimestamp
    var createdAt: Instant = Instant.now(),
    //
    @field:Column(nullable = false)
    @field:UpdateTimestamp
    var updatedAt: Instant = Instant.now(),
    //
    @field:Column(nullable = false)
    @field:ColumnDefault("0")
    @field:Version
    @field:Min(0)
    var version: Int = 0,
)
