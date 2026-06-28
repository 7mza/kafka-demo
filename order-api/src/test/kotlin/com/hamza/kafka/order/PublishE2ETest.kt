package com.hamza.kafka.order

import com.hamza.kafka.commons.parseJson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.web.servlet.client.RestTestClient
import org.springframework.test.web.servlet.client.expectBody
import org.testcontainers.kafka.KafkaContainer
import tools.jackson.databind.ObjectMapper
import java.time.Duration

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["spring.jpa.hibernate.ddl-auto=validate", "spring.liquibase.enabled=true"],
)
@Import(PgTestContainer::class, KafkaTestContainer::class)
@AutoConfigureRestTestClient
class PublishE2ETest {
    @Autowired
    private lateinit var client: RestTestClient

    @Autowired
    private lateinit var orderRepo: IOrderRepository

    @Autowired
    private lateinit var outboxRepo: IOrderOutboxRepository

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Value($$"${custom.orders.topic_name}")
    private lateinit var topicName: String

    private val consumer by lazy {
        val props =
            KafkaTestUtils.consumerProps(
                kafkaContainer.bootstrapServers,
                "order-e2e-test",
                true,
            )
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        DefaultKafkaConsumerFactory<String, String>(
            props,
            StringDeserializer(),
            StringDeserializer(),
        ).createConsumer()
    }

    @AfterEach
    fun afterEach() {
        consumer.close()
    }

    @Test
    fun `publish orders end to end integration test`() {
        // subscribe to kafka
        consumer.subscribe(listOf(topicName))

        // post 3 orders
        val requestsById =
            (1..3).associate {
                val request =
                    OrderPostDto(
                        customerId = "user_220$it",
                        items = listOf(ItemDto(sku = "sku-0$it", quantity = 10 * it, unitPriceCents = 100 * it)),
                    )
                val id =
                    client
                        .post()
                        .uri("/api/order")
                        .body(request)
                        .exchange()
                        .expectStatus()
                        .isCreated
                        .expectBody<OrderGetDto>()
                        .returnResult()
                        .responseBody!!
                        .id
                id to request
            }
        val ids = requestsById.keys.toList()

        // check orders saved to DB
        requestsById.forEach { (id, request) ->
            orderRepo.findById(id).orElse(null).also {
                assertThat(it).isNotNull
                assertThat(it!!.customerId).isEqualTo(request.customerId)
                assertThat(it.items).isEqualTo(request.items.map { item -> item.toEntity() })
            }
        }

        // check orders outbox saved to DB and published without error
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            ids.forEach {
                outboxRepo.findByOrderId(it).also { outbox ->
                    assertThat(outbox).isNotNull
                    assertThat(outbox!!.publishedAt).isNotNull
                    assertThat(outbox.attempts).isZero
                }
            }
        }

        // check events published to kafka
        val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30))
        val publishedEvents = records.records(topicName).map { parseJson<OrderPlacedEvent>(it.value(), objectMapper) }
        val expectedEvents = orderRepo.findAll().map { it.toOrderPlacedEvent() }
        assertThat(publishedEvents)
            .usingRecursiveFieldByFieldElementComparator(
                RecursiveComparisonConfiguration.builder().withIgnoredFields("eventId", "occurredAt").build(),
            ).containsExactlyInAnyOrder(*expectedEvents.toTypedArray())
    }
}
