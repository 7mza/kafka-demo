package com.hamza.kafka.order

import com.hamza.kafka.commons.parseJson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
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
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class, KafkaTestContainer::class)
class PublishServiceTest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    private val consumer by lazy {
        val props = KafkaTestUtils.consumerProps(kafkaContainer.bootstrapServers, "${UUID.randomUUID()}", true)
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "1000"
        props[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = "2000"
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
            Event(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
                totalAmountCents = 1990,
            )
        val outbox = event.toOutbox(objectMapper, topicName)

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))
        await().atMost(Duration.ofSeconds(10)).until {
            consumer.poll(Duration.ofMillis(500))
            consumer.assignment().isNotEmpty()
        }

        // publish event to kafka
        service.publish(listOf(outbox)).also {
            assertThat(it).hasSize(1)
            assertThat(it[0].publishedAt).isNotNull // check outbox published
            assertThat(it[0].attempts).isZero
        }

        // check event was sent to kafka
        KafkaTestUtils.getSingleRecord(consumer, topicName, Duration.ofSeconds(30)).also {
            assertThat(it.key()).isEqualTo(outbox.orderId)
            assertThat(parseJson<Event>(it.value(), objectMapper)).isEqualTo(event)
        }
    }
}
