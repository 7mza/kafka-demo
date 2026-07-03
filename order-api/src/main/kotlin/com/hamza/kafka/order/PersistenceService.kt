package com.hamza.kafka.order

import com.hamza.kafka.commons.DeadLetterProjection
import com.hamza.kafka.commons.ResourceNotFoundException
import jakarta.transaction.Transactional
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

interface IPersistenceService {
    fun save(request: OrderPostDto): Order

    fun getOrderById(id: String): Order

    fun getOutboxByOrderId(id: String): Outbox

    fun getDeadLetters(): List<DeadLetterProjection>
}

@Service
class PersistenceService(
    private val orderRepo: OrderRepository,
    private val orderOutboxRepo: OutboxRepository,
    private val deadLetterRepo: DeadLetterRepository,
    @Value($$"${custom.topic_name}") private val topicName: String,
) : IPersistenceService {
    @Transactional
    override fun save(request: OrderPostDto): Order {
        val order = orderRepo.save(request.toEntity())
        val event = order.toOrderPlacedEvent()
        val outbox = event.toOutbox(topicName)
        orderOutboxRepo.save(outbox)
        return order
    }

    override fun getOrderById(id: String): Order =
        orderRepo.findById(id).orElseThrow {
            ResourceNotFoundException(id = id, name = "Order")
        }

    override fun getOutboxByOrderId(id: String) =
        orderOutboxRepo.findByOrderId(id) ?: throw ResourceNotFoundException(id = id, name = "OrderOutbox")

    override fun getDeadLetters(): List<DeadLetterProjection> = deadLetterRepo.findAll()
}
