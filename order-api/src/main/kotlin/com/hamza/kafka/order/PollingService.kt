package com.hamza.kafka.order

import jakarta.transaction.Transactional
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

// low freq rescue poll
interface IPollingService {
    fun poll(): List<OrderOutbox>
}

@Service
class PollingService(
    private val repo: IOrderOutboxRepository,
    private val service: IPublishService,
    @Value($$"${custom.orders.batch_size}") private val batchSize: Int,
    @Value($$"${custom.orders.max_attempts}") private val maxAttempts: Int,
) : IPollingService {
    @Scheduled(
        initialDelayString = $$"${custom.orders.poll_initial_delay}",
        fixedDelayString = $$"${custom.orders.poll_delay}",
    )
    @Transactional
    override fun poll() =
        repo
            .retrieveUnpublished(batchSize = batchSize, maxAttempts = maxAttempts)
            .let { service.publish(it) }
}
