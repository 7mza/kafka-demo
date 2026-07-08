package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.toJson
import jakarta.transaction.Transactional
import org.apache.avro.specific.SpecificRecordBase
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper

interface IPersistenceService<T : SpecificRecordBase> {
    fun consume(event: T): Inbox
}

@Service
class PersistenceService(
    private val repo: InboxRepository,
    private val objectMapper: ObjectMapper,
) : IPersistenceService<OrderPlacedEvent> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun consume(event: OrderPlacedEvent): Inbox =
        repo
            .findById(event.eventId)
            .map { it.also { logger.warn("event '${event.toJson()}' already consumed") } }
            .orElseGet {
                repo
                    .save(event.toInbox())
                    .also { logger.info("saved new inbox '{}'", it.toJson(objectMapper)) }
            }
}
