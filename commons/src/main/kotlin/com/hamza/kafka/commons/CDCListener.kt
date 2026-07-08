package com.hamza.kafka.commons

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import java.time.Duration.ofSeconds
import javax.sql.DataSource

interface ICDCListener : ApplicationListener<ApplicationReadyEvent> {
    override fun onApplicationEvent(event: ApplicationReadyEvent)

    fun listen()
}

class CDCListener(
    private val name: String,
    private val channel: String,
    private val dataSource: DataSource,
    private val trigger: ITrigger,
) : ICDCListener {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val triggerName = AopUtils.getTargetClass(trigger).simpleName

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        Thread.ofVirtual().name("$name-listener").start(::listen)
    }

    override fun listen() {
        while (!Thread.currentThread().isInterrupted) {
            try {
                dataSource.connection.use {
                    val pgConn = it.unwrap(PGConnection::class.java)
                    it.createStatement().use { stm -> stm.execute("LISTEN $channel") }
                    logger.info("listening to DB events on channel {}", channel)
                    while (!Thread.currentThread().isInterrupted) {
                        val notifications = pgConn.getNotifications(10_000)
                        if (!notifications.isNullOrEmpty()) {
                            logger.info("received {} notifications, triggering {}", notifications.size, triggerName)
                            try {
                                trigger.trigger()
                            } catch (ex: Exception) {
                                logger.error("trigger {} failed", triggerName, ex)
                            }
                        }
                    }
                }
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (ex: Exception) {
                logger.warn("listener connection lost, reconnecting in 5s", ex)
                Thread.sleep(ofSeconds(5).toMillis())
            }
        }
    }
}
