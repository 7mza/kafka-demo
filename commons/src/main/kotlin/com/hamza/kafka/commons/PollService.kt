package com.hamza.kafka.commons

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled

interface IPollService {
    fun poll()
}

class PollService(
    private val trigger: ITrigger,
) : IPollService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(initialDelayString = $$"${custom.poll_initial_delay}", fixedDelayString = $$"${custom.poll_delay}")
    override fun poll() = trigger.trigger().also { logger.info("rescue poll triggered") }
}
