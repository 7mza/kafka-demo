package com.hamza.kafka.inventory

import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.BaseInbox
import jakarta.transaction.Transactional
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Instant

interface IProcessService<T : BaseInbox> {
    fun process(): List<T>
}

@Service
class ProcessService(
    private val inboxRepo: InboxRepository,
    private val outboxRepo: OutboxRepository,
    @Value($$"${custom.batch_size}") private val batchSize: Int,
) : IProcessService<Inbox> {
    @Transactional
    override fun process(): List<Inbox> {
        val inboxes =
            inboxRepo.retrieveUnprocessed(batchSize).onEach {
                it.status = OrderStatus.entries.random()
                it.processedAt = Instant.now()
            }
        val events = inboxes.map { it.toOrderDecidedEvent() }
        val outboxes = events.map { it.toOutbox() }
        outboxRepo.saveAll(outboxes)
        return inboxes
    }
}
