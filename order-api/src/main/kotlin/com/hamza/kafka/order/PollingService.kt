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
        initialDelayString = $$"${custom.orders.poll_initial_delay}",
        fixedDelayString = $$"${custom.orders.poll_delay}",
    )
    override fun poll() = service.trigger()
}
