package com.hamza.kafka.order

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

interface IPollingService {
    fun poll()
}

@Service
class PollingService(
    private val trigger: IDrainTrigger,
) : IPollingService {
    @Scheduled(
        initialDelayString = $$"${custom.poll_initial_delay}",
        fixedDelayString = $$"${custom.poll_delay}",
    )
    override fun poll() = trigger.trigger()
}
