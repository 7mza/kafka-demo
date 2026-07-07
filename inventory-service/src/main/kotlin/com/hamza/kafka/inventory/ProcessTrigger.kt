package com.hamza.kafka.inventory

import com.hamza.kafka.commons.ITrigger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.util.concurrent.Semaphore

@Service
class ProcessTrigger(
    private val service: IProcessService<Inbox>,
) : ITrigger {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val semaphore = Semaphore(1)

    @Async
    override fun trigger() {
        if (!semaphore.tryAcquire()) {
            logger.info("ProcessService already running")
            return
        }
        try {
            while (service.process().isNotEmpty()) { /* keep processing */ }
        } finally {
            semaphore.release()
        }
    }
}
