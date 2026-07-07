package com.hamza.kafka.inventory

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
    private val repo: InboxRepository,
    @Value($$"${custom.batch_size}") private val batchSize: Int,
) : IProcessService<Inbox> {
    @Transactional
    override fun process(): List<Inbox> =
        repo.retrieveUnprocessed(batchSize).onEach {
            it.status = Status.entries.random()
            it.processedAt = Instant.now()
        }
}
