package com.hamza.kafka.inventory

import com.hamza.commons.OrderDecidedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.BaseInbox
import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.time.Instant

interface IProcessService<T : BaseInbox> {
    fun process(): List<T>
}

@Service
class ProcessService(
    private val inboxRepo: InboxRepository,
    private val outboxRepo: OutboxRepository,
    private val serializer: IAvroSerializer<OrderDecidedEvent>,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.batch_size}") private val batchSize: Int,
    @Value($$"${custom.topics.accepted}") private val acceptedTopic: String,
    @Value($$"${custom.topics.rejected}") private val rejectedTopic: String,
) : IProcessService<Inbox> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun process(): List<Inbox> {
        val inboxes =
            inboxRepo.retrieveUnprocessed(batchSize).onEach {
                it.status = OrderStatus.entries.random()
                it.processedAt = Instant.now()
            }
        inboxes.takeIf { it.isNotEmpty() }?.also { logger.info("processed {} inbox messages", it.size) }
        val events = inboxes.map { it.toOrderDecidedEvent() }
        val outboxes =
            events.map {
                it.toOutbox(acceptedTopic = acceptedTopic, rejectedTopic = rejectedTopic, serializer = serializer)
            }
        outboxRepo
            .saveAll(outboxes)
            .takeIf { it.isNotEmpty() }
            ?.also {
                logger.info(
                    "saved {} new outbox messages [{}]",
                    it.size,
                    it.joinToString(", ") { outbox -> outbox.toJson(objectMapper) },
                )
            }
        return inboxes
    }
}
