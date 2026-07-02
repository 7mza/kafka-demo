package com.hamza.kafka.order

import com.hamza.kafka.commons.KafkaPublishResult
import jakarta.transaction.Transactional
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

interface IDrainService {
    fun drain(): KafkaPublishResult?
}

@Service
class DrainService(
    private val repo: OutboxRepository,
    private val service: IPublishService,
    @Value($$"${custom.batch_size}") private val batchSize: Int,
    @Value($$"${custom.max_attempts}") private val maxAttempts: Int,
) : IDrainService {
    @Transactional
    override fun drain(): KafkaPublishResult? {
        val batch = repo.retrieveUnpublished(batchSize, maxAttempts)
        if (batch.isEmpty()) return null
        return service.publish(batch)
    }
}
