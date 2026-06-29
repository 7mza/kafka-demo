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
    private val orderRepo: IOrderRepository,
    private val orderOutboxRepo: IOrderOutboxRepository,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.orders.topic_name}") private val topicName: String,
) : IPersistenceService {
    @Transactional
    override fun save(request: OrderPostDto): Order {
        val order = orderRepo.save(request.toEntity())
        val event = order.toOrderPlacedEvent()
        val outbox = event.toOrderOutbox(objectMapper, topicName)
        orderOutboxRepo.save(outbox)
        return order
    }

    override fun getOrderById(id: String): Order =
        orderRepo.findById(id).orElseThrow {
            ResourceNotFoundException(id = id, name = "Order")
        }

    override fun getOutboxByOrderId(id: String) =
        orderOutboxRepo.findByOrderId(id) ?: throw ResourceNotFoundException(id = id, name = "OrderOutbox")
}
