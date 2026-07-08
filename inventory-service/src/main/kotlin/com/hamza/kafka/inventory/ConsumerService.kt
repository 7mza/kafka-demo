package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.IConsumerService
import com.hamza.kafka.commons.toJson
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

@Service
class ConsumerService(
    private val repo: InboxRepository,
    private val objectMapper: ObjectMapper,
) : IConsumerService<OrderPlacedEvent, Inbox> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun consume(event: OrderPlacedEvent): Inbox {
        repo.findById(event.eventId).orElse(null)?.let {
            logger.warn("event '{}' already consumed", event.toJson())
            return it
        }

        val inbox = repo.save(event.toInbox())
        logger.info("saved new inbox '{}'", inbox.toJson(objectMapper))
        return inbox
    }
}
