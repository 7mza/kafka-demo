package com.hamza.kafka.order

import com.hamza.kafka.commons.parseJson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.testcontainers.kafka.KafkaContainer
import tools.jackson.databind.ObjectMapper
import java.time.Duration

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class, KafkaTestContainer::class)
class PublishServiceTest {
    @Autowired
    private lateinit var service: IPublishService

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
                "publish-service-test",
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
    fun `publish should send order to kafka and mark related outbox as published`() {
        // gen event + outbox
        val event =
            OrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
                totalAmountCents = 1990,
            )
        val outbox = event.toOrderOutbox(objectMapper, topicName)

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))

        // publish event to kafka
        service.publish(listOf(outbox)).also {
            assertThat(it).hasSize(1)
            assertThat(it[0].publishedAt).isNotNull // check outbox published
            assertThat(it[0].attempts).isZero
        }

        // check event was sent to kafka
        KafkaTestUtils.getSingleRecord(consumer, topicName, Duration.ofSeconds(30)).also {
            assertThat(it.key()).isEqualTo(outbox.orderId)
            assertThat(parseJson<OrderPlacedEvent>(it.value(), objectMapper)).isEqualTo(event)
        }
    }
}
