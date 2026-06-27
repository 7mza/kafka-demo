package com.hamza.kafka.order

import com.hamza.kafka.commons.ResourceNotFoundException
import jakarta.transaction.Transactional
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

interface IPersistenceService {
    fun save(request: OrderPostDto): Order

    fun getOrderById(id: String): Order

    fun getOutboxByOrderId(id: String): OrderOutbox
}

@Service
class PersistenceService(
    private val orderRepository: IOrderRepository,
    private val orderOutboxRepository: IOrderOutboxRepository,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.orders.topic_name}") private val topicName: String,
) : IPersistenceService {
    @Transactional
    override fun save(request: OrderPostDto): Order {
        val order = orderRepository.save(request.toEntity())
        val event = order.toOrderPlacedEvent()
        val outbox = event.toOrderOutbox(objectMapper, topicName)
        orderOutboxRepository.save(outbox)
        return order
    }

    override fun getOrderById(id: String): Order =
        orderRepository.findById(id).orElseThrow {
            ResourceNotFoundException(id = id, name = "Order")
        }

    override fun getOutboxByOrderId(id: String): OrderOutbox =
        orderOutboxRepository.findByOrderId(id) ?: throw ResourceNotFoundException(id = id, name = "OrderOutbox")
}
