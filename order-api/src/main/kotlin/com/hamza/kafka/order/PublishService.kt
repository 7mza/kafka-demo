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

data class PublishResult(
    var publishedCount: Int = 0,
    var recoverableErrorsCount: Int = 0,
    var deadLettersCount: Int = 0,
) {
    // progress = getting outboxes off the backlog (whether sent to kafka or flagged as dead letters)
    fun isProgressing() = this.publishedCount + this.deadLettersCount > 0

    // used for simple linear time back off based on transient errors (if something is wrong with kafka, give it time to heal)
    fun hasRecoverable() = this.recoverableErrorsCount > 0
}

interface IPublishService {
    fun publish(orders: List<Outbox>): PublishResult
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
    override fun publish(orders: List<Outbox>): PublishResult {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val result = PublishResult()
        try {
            val pending =
                orders.map {
                    it to
                        executor.submit<SendResult<String, Event>> {
                            // exception here will only fail this task future, other tasks are not affected
                            // exception sits on this future until .get() below is called
                            val event = parseJson<Event>(it.payload, objectMapper)
                            // for demo: force permanent ex so this outbox dead letters after max_attempts
                            check(!event.customerId.contains("fail")) { "forced failure for demo" }
                            kafkaTemplate
                                .send(it.topic, it.orderId, event)
                                // how long to wait for Kafka to ack write
                                .get(publishTimeout.toNanos(), TimeUnit.NANOSECONDS)
                        }
                }
            pending.forEach { (outbox, future) ->
                try {
                    // backstop if .send() above is stuck before the timeout start counting
                    // normally this should return instantly since task is already done
                    future.get(publishTimeout.toNanos(), TimeUnit.NANOSECONDS)
                    result.publishedCount++
                    outbox.publishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS)
                } catch (ex: Exception) {
                    // catch both: task failure + backstop (giving up waiting on it)
                    val rootCause = ExceptionUtils.getRootCause(ex) ?: ex
                    if (rootCause is RetriableException || rootCause is TimeoutException) {
                        result.recoverableErrorsCount++
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
                            result.deadLettersCount++
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
                }
            }
            return result
        } finally {
            executor.shutdownNow()
        }
    }
}
