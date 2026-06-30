package com.hamza.kafka.order

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

interface IPollingService {
    fun poll()
}

@Service
class PollingService(
    private val service: IDrainServiceTrigger,
) : IPollingService {
    @Scheduled(
        initialDelayString = $$"${custom.poll_initial_delay}",
        fixedDelayString = $$"${custom.poll_delay}",
    )
    override fun poll() = service.trigger()
}
