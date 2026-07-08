package com.hamza.kafka.order

import com.hamza.kafka.commons.DeadLetterProjection
import com.hamza.kafka.commons.ResourceNotFoundException
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

interface IPersistenceService {
    fun save(request: OrderPostDto): Order

    fun getOrderById(id: String): Order

    fun getDeadLetters(): List<DeadLetterProjection>
}

@Service
class PersistenceService(
    private val orderRepo: OrderRepository,
    private val orderOutboxRepo: OutboxRepository,
    private val deadLetterRepo: DeadLetterRepository,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.topics.placed}") private val topicName: String,
) : IPersistenceService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun save(request: OrderPostDto): Order {
        val order = orderRepo.save(request.toEntity())
        val event = order.toOrderPlacedEvent()
        val outbox = event.toOutbox(topicName)
        orderOutboxRepo.save(outbox).also { logger.info("saved new outbox '{}'", it.toJson(objectMapper)) }
        return order
    }

    override fun getOrderById(id: String): Order =
        orderRepo.findById(id).orElseThrow { ResourceNotFoundException(id = id, name = "Order") }

    override fun getDeadLetters(): List<DeadLetterProjection> = deadLetterRepo.findAll()
}
