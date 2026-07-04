package com.hamza.kafka.order

import com.hamza.kafka.avro.OrderPlacedEvent
import com.hamza.kafka.commons.KafkaPublishResult
import com.hamza.kafka.commons.fromJson
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import jakarta.transaction.Transactional
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.errors.RetriableException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

interface IPublishService {
    fun publish(orders: List<Outbox>): KafkaPublishResult
}

@Service
class PublishService(
    private val kafkaTemplate: KafkaTemplate<String, OrderPlacedEvent>,
    @Value($$"${custom.publish_timeout}") private val publishTimeout: Duration,
    @Value($$"${custom.max_attempts}") private val maxAttempts: Int,
) : IPublishService {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun publish(orders: List<Outbox>): KafkaPublishResult {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val result = KafkaPublishResult()
        try {
            val pending =
                orders.map {
                    it to
                        executor.submit<SendResult<String, OrderPlacedEvent>> {
                            // exception here will only fail this task future, other tasks are not affected
                            // exception sits on this future until .get() below is called
                            val event = fromJson<OrderPlacedEvent>(it.payload)
                            // for demo: force dead letter
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
                    if (ex.isTransientFailure()) {
                        result.recoverableErrorsCount++
                        // transient ex: don't inc attempts, next polls should retry
                        logger.warn(
                            "transient failure for outbox: '{}', topic: '{}', will retry",
                            outbox.id,
                            outbox.topic,
                            ex,
                        )
                    } else {
                        // permanent ex: inc attempts, next polls should retry with upper limit
                        if (ex.message?.contains("forced failure for demo") ?: false) { // for demo: force dead letter
                            outbox.attempts = maxAttempts
                        } else {
                            outbox.attempts++
                        }
                        if (outbox.attempts >= maxAttempts) {
                            result.deadLettersCount++
                            logger.error(
                                "dead letter outbox: '{}', topic: '{}', excluded from future polls",
                                outbox.id,
                                outbox.topic,
                                ex,
                            )
                            val rootCause = ExceptionUtils.getRootCause(ex) ?: ex
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

/*
 * transient/recoverable failure:
 *  RetriableException: kafka errors (isr below min_inSync, leader election, ...)
 *  TimeoutException: this service ACK backstop
 *  IOException: schema registry unreachable
 *  RestClientException 5xx: schema registry reachable but unhealthy
 *
 * permanent/non-recoverable failure:
 *  anything else (bad payload, incompatible schemas / RestClientException 4xx, unhandled)
 *
 * ex scan because
 *  registry not reachable throw RestClientException 5xx = transient
 *  invalid schema throw RestClientException 4xx = permanent
 */
fun Throwable.isTransientFailure(): Boolean =
    ExceptionUtils.getThrowableList(this).any {
        it is RetriableException ||
            it is TimeoutException ||
            it is IOException ||
            (it is RestClientException && it.status >= 500)
    }
