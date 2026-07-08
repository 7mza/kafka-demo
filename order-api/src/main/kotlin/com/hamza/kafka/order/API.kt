package com.hamza.kafka.order

import com.hamza.kafka.commons.DeadLettersDto
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
import org.springframework.web.util.UriComponentsBuilder

@Tag(name = "order", description = "orders ops")
@RequestMapping(value = ["/api/order"], produces = [MediaType.APPLICATION_JSON_VALUE])
interface API {
    @PostMapping
    @Operation(
        summary = "Create a single order",
        description = "Can force a dead letter outbox by using string `fail` in customerId",
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
  ],
  "status": "PENDING"
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
  ],
  "status": "PENDING"
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

    @GetMapping("/dl")
    @Operation(
        summary = "Get dead letter outbox messages",
        description = "Outbox messages that exceeded `max_attempts` & are excluded from any future publishing cycle",
    )
    @ApiResponses(
        value = [
            ApiResponse(
                responseCode = "200",
                description = "OK",
                content = [
                    Content(
                        mediaType = MediaType.APPLICATION_JSON_VALUE,
                        schema = Schema(implementation = DeadLettersDto::class),
                        examples = [
                            ExampleObject(
                                name = "example-0",
                                value = """
{
  "results": [
    {
      "id": "0qtc1hmz3p6nk",
      "orderId": "0qsbs74grkjq2",
      "eventType": "order.placed",
      "topic": "orders.placed",
      "payload": "{\"items\": [{\"sku\": \"sku-01\", \"quantity\": 10, \"unitPriceCents\": 199}], \"eventId\": \"0qtc1hmz3p6nk\", \"orderId\": \"0qsbs74grkjq2\", \"eventType\": \"order.placed\", \"customerId\": \"fail_2203\", \"occurredAt\": \"2026-07-02T12:49:45.154Z\", \"totalAmountCents\": 1990}",
      "attempts": 10,
      "lastError": "IllegalStateException: forced failure for demo",
      "createdAt": "2026-07-02T12:49:45.196850Z",
      "lastErrorAt": "2026-07-02T12:49:45.346726Z"
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
    fun getDeadLetters(): DeadLettersDto
}
