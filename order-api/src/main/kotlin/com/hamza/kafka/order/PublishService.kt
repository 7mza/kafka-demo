package com.hamza.kafka.order

import com.hamza.kafka.commons.parseJson
import jakarta.transaction.Transactional
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.errors.RetriableException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

interface IPublishService {
    fun publish(orders: List<Outbox>): List<Outbox>
}

@Service
class PublishService(
    private val kafkaTemplate: KafkaTemplate<String, Event>,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.publish_timeout}") private val publishTimeout: Duration,
    @Value($$"${custom.max_attempts}") private val maxAttempts: Int,
) : IPublishService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun publish(orders: List<Outbox>): List<Outbox> {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        try {
            val pending =
                orders.map {
                    it to
                        executor.submit<SendResult<String, Event>> {
                            // exception here will only fail this task future, other tasks are not affected
                            // exception sits on this future until .get() below is called
                            kafkaTemplate
                                .send(it.topic, it.orderId, parseJson(it.payload, objectMapper))
                                // how long to wait for Kafka to ack write
                                .get(publishTimeout.toNanos(), TimeUnit.NANOSECONDS)
                        }
                }
            return pending.mapNotNull { (outbox, future) ->
                try {
                    // backstop if .send() above is stuck before the timeout start counting
                    // normally this should return instantly since task is already done
                    future.get(publishTimeout.toNanos(), TimeUnit.NANOSECONDS)
                    outbox.publishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS)
                    outbox
                } catch (ex: Exception) {
                    // catch both: task failure + backstop (giving up waiting on it)
                    val rootCause = ExceptionUtils.getRootCause(ex) ?: ex
                    if (rootCause is RetriableException || rootCause is TimeoutException) {
                        // transient ex (broker unreachable, isr below min_inSync, leader election in progress, ...)
                        // don't inc attempts, next polls should retry
                        logger.warn(
                            "transient failure for outbox: '{}', topic: '{}', will retry",
                            outbox.id,
                            outbox.topic,
                            ex,
                        )
                    } else {
                        // permanent ex (bad payload, invalid topic, serialization error, ...)
                        // inc attempts, next polls should retry with upper limit
                        outbox.attempts++
                        if (outbox.attempts >= maxAttempts) {
                            logger.error(
                                "dead letter outbox: '{}', topic: '{}', excluded from future polls",
                                outbox.id,
                                outbox.topic,
                                ex,
                            )
                            val simpleName = rootCause.javaClass.simpleName
                            outbox.lastError =
                                rootCause.message?.takeIf { it.isNotBlank() }?.let { "$simpleName: $it" } ?: simpleName
                        } else {
                            logger.error("failed to publish outbox: '{}', topic: '{}'", outbox.id, outbox.topic, ex)
                        }
                    }
                    null
                }
            }
        } finally {
            executor.shutdownNow()
        }
    }
}
