package com.hamza.kafka.order

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.util.UriComponentsBuilder

@Tag(name = "order", description = "orders ops")
@RequestMapping(value = ["/api/order"], produces = [MediaType.APPLICATION_JSON_VALUE])
interface IApi {
    @PostMapping
    @Operation(
        summary = "Create a single order",
        description = """
In same transaction, will create order entry + event/outbox entry in DB.<br/>
Publishing poller will pick it in batches in next polls.
""",
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "201",
                description = "CREATED",
                content = [
                    Content(
                        mediaType = MediaType.APPLICATION_JSON_VALUE,
                        schema = Schema(OrderGetDto::class),
                        examples = [
                            ExampleObject(
                                name = "example-0",
                                value = """
{
  "id": "0qsbs74grkjq2",
  "customerId": "user_2203",
  "createdAt": "2026-06-23T11:44:28Z",
  "items": [
    {
      "sku": "sku-01",
      "quantity": 10,
      "unitPriceCents": 199
    }
  ]
}
""",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun save(
        @RequestBody @Valid request: OrderPostDto,
        uriBuilder: UriComponentsBuilder,
    ): ResponseEntity<OrderGetDto>

    @GetMapping("{id}")
    @Operation(summary = "Get an order by id", description = "")
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "OK",
                content = [
                    Content(
                        mediaType = MediaType.APPLICATION_JSON_VALUE,
                        schema = Schema(OrderGetDto::class),
                        examples = [
                            ExampleObject(
                                name = "example-0",
                                value = """
{
  "id": "0qsbs74grkjq2",
  "customerId": "user_2203",
  "createdAt": "2026-06-23T11:44:28Z",
  "items": [
    {
      "sku": "sku-01",
      "quantity": 10,
      "unitPriceCents": 199
    }
  ]
}
""",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun getOrderById(
        @Parameter(description = "order id") @PathVariable @NotBlank @Size(min = 13, max = 13) id: String,
    ): OrderGetDto

    @GetMapping("/outbox/{id}")
    @Operation(summary = "Get an order outbox message by orderId", description = "")
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "OK",
                content = [
                    Content(
                        mediaType = MediaType.APPLICATION_JSON_VALUE,
                        schema = Schema(OrderOutboxDto::class),
                        examples = [
                            ExampleObject(
                                name = "example-0",
                                value = """
{
  "id": "0qtc1hmz3p6nk",
  "orderId": "0qsbs74grkjq2",
  "eventType": "order.placed",
  "topic": "orders",
  "payload": "{\"items\": [{\"sku\": \"sku-01\", \"quantity\": 10, \"unitPriceCents\": 199}], \"eventId\": \"0qtc1hmz3p6nk\", \"orderId\": \"0qsbs74grkjq2\", \"eventType\": \"order.placed\", \"customerId\": \"user_2203\", \"occurredAt\": \"2026-06-26T14:54:47.544Z\", \"totalAmountCents\": 1990}",
  "publishedAt": "2026-06-26T14:54:57.536Z",
  "attempts": 0,
  "lastError": null
}
""",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    fun getOutboxByOrderId(
        @Parameter(description = "order id") @PathVariable @NotBlank @Size(min = 13, max = 13) id: String,
    ): OrderOutboxDto
}

@RestController
class Ctrl(
    private val service: IPersistenceService,
) : IApi {
    override fun save(
        request: OrderPostDto,
        uriBuilder: UriComponentsBuilder,
    ): ResponseEntity<OrderGetDto> {
        val response = service.save(request).toDto()
        val uri = uriBuilder.path("/api/order/{id}").buildAndExpand(response.id).toUri()
        return ResponseEntity.created(uri).body(response)
    }

    override fun getOrderById(id: String) = service.getOrderById(id).toDto()

    override fun getOutboxByOrderId(id: String) = service.getOutboxByOrderId(id).toDto()
}
