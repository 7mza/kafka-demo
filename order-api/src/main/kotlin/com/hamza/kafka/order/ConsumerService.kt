package com.hamza.kafka.order

import com.hamza.commons.OrderDecidedEvent
import com.hamza.kafka.commons.IConsumerService
import com.hamza.kafka.commons.ResourceNotFoundException
import com.hamza.kafka.commons.toJson
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

@Service
class ConsumerService(
    private val orderRepo: OrderRepository,
    private val inboxRepo: InboxRepository,
    private val objectMapper: ObjectMapper,
) : IConsumerService<OrderDecidedEvent, Inbox> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun consume(event: OrderDecidedEvent): Inbox {
        inboxRepo.findById(event.eventId).orElse(null)?.let {
            logger.warn("event '{}' already consumed", event.toJson())
            return it
        }

        val inbox = inboxRepo.save(event.toInbox())
        logger.info("saved new inbox '{}'", inbox.toJson(objectMapper))

        val order =
            orderRepo
                .findById(inbox.orderId)
                .orElseThrow { ResourceNotFoundException(id = inbox.orderId, name = "Order") }
        order.status = event.status

        return inbox
    }
}
