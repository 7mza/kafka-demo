package com.hamza.kafka.order

import com.hamza.kafka.commons.IDrainBackOff
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.util.concurrent.Semaphore

interface IDrainTrigger {
    fun trigger()
}

@Service
class DrainTrigger(
    private val service: IDrainService,
    private val backOff: IDrainBackOff,
) : IDrainTrigger {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val semaphore = Semaphore(1)

    @Async
    override fun trigger() {
        if (!semaphore.tryAcquire()) {
            logger.info("DrainService already running")
            return
        }
        try {
            while (true) {
                if (backOff.isActive()) break
                val result = service.drain() ?: break
                backOff.observe(result)
            }
        } finally {
            semaphore.release()
        }
    }
}
