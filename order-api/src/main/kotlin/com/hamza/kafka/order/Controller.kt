package com.hamza.kafka.order

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.util.UriComponentsBuilder

@RestController
class Controller(
    private val service: IPersistenceService,
) : API {
    override fun save(
        request: OrderPostDto,
        uriBuilder: UriComponentsBuilder,
    ): ResponseEntity<OrderGetDto> {
        val response = service.save(request).toDto()
        val uri = uriBuilder.path("/api/order/{id}").buildAndExpand(response.id).toUri()
        return ResponseEntity.created(uri).body(response)
    }

    override fun getOrderById(id: String) = service.getOrderById(id).toDto()

    override fun getOutboxByOrderId(id: String) = service.getOutboxByOrderId(id).toDto()

    override fun getDeadLetters() = service.getDeadLetters().map { it.toDto() }.let { DeadLettersDto(it) }
}
