package com.hamza.kafka.commons

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(HttpStatus.NOT_FOUND)
data class ResourceNotFoundException(
    private val id: String,
    private val name: String,
) : RuntimeException("$name: $id not found")

internal class Exceptions
