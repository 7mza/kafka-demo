package com.hamza.kafka.order

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component
import javax.sql.DataSource

interface IOutboxListener : ApplicationListener<ApplicationReadyEvent> {
    override fun onApplicationEvent(event: ApplicationReadyEvent)

    fun listen()
}

@Component
class OutboxListener(
    private val dataSource: DataSource,
    private val service: IDrainServiceTrigger,
) : IOutboxListener {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        Thread
            .ofVirtual()
            .name("outbox-listener")
            .start(::listen)
    }

    override fun listen() {
        while (!Thread.currentThread().isInterrupted) {
            try {
                dataSource.connection.use {
                    val pgConn = it.unwrap(PGConnection::class.java)
                    it.createStatement().use { stm -> stm.execute("LISTEN outbox_channel") }
                    logger.info("Listening to DB events on channel: outbox_channel")
                    while (!Thread.currentThread().isInterrupted) {
                        val notifications = pgConn.getNotifications(10_000)
                        if (!notifications.isNullOrEmpty()) {
                            logger.info("Received {} notifications, triggering DrainService", notifications.size)
                            service.trigger()
                        }
                    }
                }
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (_: Exception) {
                logger.warn("Listener connection lost, reconnecting in 5s")
                Thread.sleep(5_000)
            }
        }
    }
}
