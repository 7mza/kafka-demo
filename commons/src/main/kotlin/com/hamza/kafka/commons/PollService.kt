package com.hamza.kafka.commons

import org.springframework.scheduling.annotation.Scheduled

interface IPollService {
    fun poll()
}

class PollService(
    private val trigger: ITrigger,
) : IPollService {
    @Scheduled(initialDelayString = $$"${custom.poll_initial_delay}", fixedDelayString = $$"${custom.poll_delay}")
    override fun poll() = trigger.trigger()
}
