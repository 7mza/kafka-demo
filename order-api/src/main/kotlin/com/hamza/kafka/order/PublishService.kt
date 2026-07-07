package com.hamza.kafka.order

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.BaseOutbox
import com.hamza.kafka.commons.KafkaPublishResult
import com.hamza.kafka.commons.fromJson
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import jakarta.transaction.Transactional
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.errors.RetriableException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

interface IPublishService<T : BaseOutbox> {
    fun publish(orders: List<T>): KafkaPublishResult
}

@Service
class PublishService(
    private val kafkaTemplate: KafkaTemplate<String, OrderPlacedEvent>,
    private val objectMapper: ObjectMapper,
    @Value($$"${custom.publish_timeout}") private val publishTimeout: Duration,
    @Value($$"${custom.max_attempts}") private val maxAttempts: Int,
) : IPublishService<Outbox> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun publish(orders: List<Outbox>): KafkaPublishResult {
        val result = KafkaPublishResult()
        val pending =
            orders.map {
                it to
                    runCatching {
                        val event = fromJson<OrderPlacedEvent>(it.payload)
                        // for demo: force dead letter
                        check(!event.customerId.contains("fail")) { "forced failure for demo" }
                        kafkaTemplate.send(it.topic, it.orderId, event)
                    }.getOrElse { ex -> CompletableFuture.failedFuture(ex) }
            }
        pending.forEach { (outbox, future) ->
            try {
                // how long to wait for Kafka to ack write
                future.get(publishTimeout.toNanos(), TimeUnit.NANOSECONDS)
                result.publishedCount++
                outbox.publishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            } catch (ex: Exception) {
                if (ex.isTransientFailure()) { // transient ex: don't inc attempts, next polls should retry
                    result.recoverableErrorsCount++
                    logger.warn("transient failure for outbox '{}', will retry", outbox.toJson(objectMapper), ex)
                } else { // permanent ex: inc attempts, next polls should retry with upper limit
                    if (ex.message?.contains("forced failure for demo") ?: false) { // for demo: force dead letter
                        outbox.attempts = maxAttempts
                    } else {
                        outbox.attempts++
                    }
                    if (outbox.attempts >= maxAttempts) {
                        result.deadLettersCount++
                        logger.error(
                            "dead letter outbox '{}', excluded from future cycles",
                            outbox.toJson(objectMapper),
                            ex,
                        )
                        val rootCause = ExceptionUtils.getRootCause(ex) ?: ex
                        val simpleName = rootCause.javaClass.simpleName
                        outbox.lastError =
                            rootCause.message?.takeIf { it.isNotBlank() }?.let { "$simpleName: $it" } ?: simpleName
                    } else {
                        logger.warn("permanent failure for outbox '{}', will retry", outbox.toJson(objectMapper), ex)
                    }
                }
            }
        }
        return result
    }
}

/*
 * transient/recoverable failure:
 *  RetriableException: kafka errors (isr below min_inSync, leader election, ...)
 *  TimeoutException: kafka ACK timeout // FIXME: does kafka throws Timeout or Retriable
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
