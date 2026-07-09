package com.hamza.kafka.inventory

import com.hamza.kafka.commons.CDCListener
import com.hamza.kafka.commons.ICDCListener
import com.hamza.kafka.commons.IPollService
import com.hamza.kafka.commons.ITrigger
import com.hamza.kafka.commons.PollService
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ansi.AnsiColor
import org.springframework.boot.ansi.AnsiOutput
import org.springframework.boot.ansi.AnsiStyle
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.EnableScheduling
import java.net.InetAddress
import javax.sql.DataSource

@Configuration
@EnableScheduling
class Configurations(
    @Value($$"${server.port}") private val port: Int,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @EventListener(ApplicationReadyEvent::class)
    fun readyListener() {
        val address = "http://${InetAddress.getLocalHost().hostAddress}:$port"
        val message = "inventory-service running at $address"
        logger.info(AnsiOutput.toString(AnsiColor.BRIGHT_GREEN, AnsiStyle.BOLD, message))
    }

    @Bean
    fun cdcListener(
        dataSource: DataSource,
        trigger: ITrigger,
    ): ICDCListener =
        CDCListener(
            name = "inbox",
            channel = "inbox_channel",
            dataSource = dataSource,
            trigger = trigger,
        )

    @Bean
    fun pollService(trigger: ITrigger): IPollService = PollService(trigger)
}
