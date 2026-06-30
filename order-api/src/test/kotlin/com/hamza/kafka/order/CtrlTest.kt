package com.hamza.kafka.order

import com.hamza.kafka.commons.ResourceNotFoundException
import com.hamza.kafka.commons.writeJson
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import tools.jackson.databind.ObjectMapper

@WebMvcTest(controllers = [Ctrl::class])
@AutoConfigureRestTestClient
class CtrlTest {
    @MockitoBean
    private lateinit var service: IPersistenceService

    @Autowired
    private lateinit var client: RestTestClient

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun `create valid order and check successful 201`() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        val response =
            client
                .post()
                .uri("/api/order")
                .contentType(MediaType.APPLICATION_JSON)
                .body(json)
                .exchange()
                .expectStatus()
                .isCreated
                .expectHeader()
                .valueMatches("Location", ".*/api/order/${order.id}")
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON)
                .expectBody<OrderGetDto>()
                .returnResult()

        verify(service).save(eq(request))
        assertThat(response.responseBody).isEqualTo(order.toDto())
    }

    @Test
    fun `create invalid order - customerId @NotBlank - and check 400`() {
        val order =
            Order(customerId = "   ", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun `create invalid order - customerId @NotEmpty - and check 400`() {
        val order =
            Order(customerId = "", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun `create invalid order - customerId @Size(max=100) - and check 400`() {
        val order =
            Order(
                customerId = "1".repeat(101),
                items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
            )
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun `create invalid order - items @NotEmpty - and check 400`() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))
        val request = OrderPostDto(customerId = order.customerId, items = emptyList())
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun `create invalid order - quantity @Min(1) - and check 400`() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 0, unitPriceCents = 199)))
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun `create invalid order - unitPriceCents @Positive - and check 400`() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = -1)))
        val request = OrderPostDto(customerId = order.customerId, items = order.items.map { it.toDto() })
        val json = writeJson(request, objectMapper)

        whenever(service.save(any<OrderPostDto>())).thenReturn(order)

        client
            .post()
            .uri("/api/order")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .exchange()
            .expectStatus()
            .isBadRequest

        verify(service, never()).save(eq(request))
    }

    @Test
    fun getOrderById() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))

        whenever(service.getOrderById(anyString())).thenReturn(order)

        val response =
            client
                .get()
                .uri("/api/order/${order.id}")
                .exchange()
                .expectStatus()
                .isOk
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON)
                .expectBody<OrderGetDto>()
                .returnResult()

        verify(service).getOrderById(eq(order.id))
        assertThat(response.responseBody).isEqualTo(order.toDto())
    }

    @Test
    fun getOrderByWrongId() {
        doThrow(ResourceNotFoundException(id = "id", name = "name")).whenever(service).getOrderById(anyString())

        client
            .get()
            .uri("/api/order/1111111111111")
            .exchange()
            .expectStatus()
            .isNotFound
    }

    @Test
    fun getOutboxByOrderId() {
        val order =
            Order(customerId = "user-2203", items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)))
        val outbox = order.toOrderPlacedEvent().toOutbox(objectMapper, "")

        whenever(service.getOutboxByOrderId(anyString())).thenReturn(outbox)

        val response =
            client
                .get()
                .uri("/api/order/outbox/${order.id}")
                .exchange()
                .expectStatus()
                .isOk
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON)
                .expectBody<OrderOutboxDto>()
                .returnResult()

        verify(service).getOutboxByOrderId(eq(order.id))
        assertThat(response.responseBody).isEqualTo(outbox.toDto())
    }

    @Test
    fun getOutboxByWrongOrderId() {
        doThrow(ResourceNotFoundException(id = "id", name = "name")).whenever(service).getOutboxByOrderId(anyString())

        client
            .get()
            .uri("/api/order/outbox/1111111111111")
            .exchange()
            .expectStatus()
            .isNotFound
    }
}
