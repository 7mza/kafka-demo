package com.hamza.kafka.order

import jakarta.transaction.Transactional
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.util.concurrent.Semaphore

interface IDrainServiceTrigger {
    fun trigger()
}

@Service
class DrainServiceTrigger(
    private val service: IDrainService,
) : IDrainServiceTrigger {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val semaphore = Semaphore(1)

    @Async
    override fun trigger() {
        if (!semaphore.tryAcquire()) {
            logger.info("DrainService already running")
            return
        }
        try {
            while (service.drainOutboxes()) { /* keep going until outbox empty */ }
        } finally {
            semaphore.release()
        }
    }
}

interface IDrainService {
    fun drainOutboxes(): Boolean
}

@Service
class DrainService(
    private val repo: OutboxRepository,
    private val service: IPublishService,
    @Value($$"${custom.batch_size}") private val batchSize: Int,
    @Value($$"${custom.max_attempts}") private val maxAttempts: Int,
) : IDrainService {
    @Transactional
    override fun drainOutboxes() =
        repo
            .retrieveUnpublished(batchSize, maxAttempts)
            .takeIf { it.isNotEmpty() }
            ?.also { service.publish(it) } != null
}
