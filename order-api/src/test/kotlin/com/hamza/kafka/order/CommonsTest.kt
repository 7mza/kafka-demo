package com.hamza.kafka.order

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.json.JsonTest
import tools.jackson.databind.ObjectMapper

// for GraalVM tracing-agent to intercept commons
@JsonTest
class CommonsTest {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val json =
        """
{"id":"0qsbs74grkjq2","customerId":"user_2203","createdAt":"2026-06-23T11:44:28Z","items":[{"sku":"sku-01","quantity":10,"unitPriceCents":199}]}
        """.trimIndent()

    private val order =
        OrderGetDto(
            id = "0qsbs74grkjq2",
            customerId = "user_2203",
            createdAt = "2026-06-23T11:44:28Z",
            items = listOf(ItemDto(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @Test
    fun parseJson() {
        com.hamza.kafka.commons.parseJson<OrderGetDto>(json, objectMapper).also {
            assertThat(it).isEqualTo(order)
        }
    }

    @Test
    fun writeJson() {
        com.hamza.kafka.commons.writeJson(order, objectMapper).also {
            assertThat(it).isEqualTo(json)
        }
    }
}
