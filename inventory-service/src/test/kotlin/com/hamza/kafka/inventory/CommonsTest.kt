package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import com.hamza.kafka.commons.fromJson
import com.hamza.kafka.commons.parseJson
import com.hamza.kafka.commons.toJson
import com.hamza.kafka.commons.writeJson
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.json.JsonTest
import tools.jackson.databind.ObjectMapper
import java.time.Instant

// placed here instead of commons for GraalVM tracing-agent to intercept it
@JsonTest
class CommonsTest {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val inboxJson =
        """
{"orderId":"order_2203","eventType":"eventType","payload":"{\"id\":\"0qsbs74grkjq2\",\"customerId\":\"user_2203\",\"createdAt\":\"2026-06-23T11:44:28Z\",\"items\":[{\"sku\":\"sku-01\",\"quantity\":10,\"unitPriceCents\":199}]}","status":"ACCEPTED","createdAt":"2026-01-01T00:00:00Z","id":"0qsbs74grkjq2","processedAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z","version":0}
        """.trimIndent()

    private val inbox =
        Inbox(
            orderId = "order_2203",
            eventType = "eventType",
            payload =
                """
{"id":"0qsbs74grkjq2","customerId":"user_2203","createdAt":"2026-06-23T11:44:28Z","items":[{"sku":"sku-01","quantity":10,"unitPriceCents":199}]}
                """.trimIndent(),
            status = OrderStatus.ACCEPTED,
        ).apply {
            id = "0qsbs74grkjq2"
            processedAt = Instant.parse("2026-01-01T00:00:00Z")
            createdAt = Instant.parse("2026-01-01T00:00:00Z")
            updatedAt = Instant.parse("2026-01-01T00:00:00Z")
        }

    private val eventJson =
        """
{"eventId":"0qsbs74grkjq2","occurredAt":1767225600000,"orderId":"order_2203","customerId":"user_2203","items":[{"sku":"sku-01","quantity":10,"unitPriceCents":199}],"totalAmountCents":1990}
        """.trimIndent()

    private val event =
        createOrderPlacedEvent(
            evenId = "0qsbs74grkjq2",
            occurredAt = Instant.parse("2026-01-01T00:00:00Z"),
            orderId = "order_2203",
            customerId = "user_2203",
            items = listOf(createEventItem(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @Test
    fun `parseJson should transform a json string to it's original object using jackson`() {
        parseJson<Inbox>(inboxJson, objectMapper).also { assertThat(it).isEqualTo(inbox) }
    }

    @Test
    fun `writeJson should transform an object to json string using jackson`() {
        writeJson(inbox, objectMapper).also { assertThat(it).isEqualTo(inboxJson) }
    }

    @Test
    fun `toJson should transform a SpecificRecordBase to json string using avro serialiser`() {
        event.toJson().also { assertThat(it).isEqualTo(eventJson) }
    }

    @Test
    fun `fromJson should transform a json string to it's original SpecificRecordBase using avro deserializer`() {
        fromJson<OrderPlacedEvent>(eventJson).also { assertThat(it).isEqualTo(event) }
    }
}
