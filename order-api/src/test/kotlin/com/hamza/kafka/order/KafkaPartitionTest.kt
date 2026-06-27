package com.hamza.kafka.order

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

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["custom.orders.partitions=3"],
)
@Import(PgTestContainer::class, KafkaTestContainer::class)
class KafkaPartitionTest {
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
    fun `same orderId = same partition`() {
        // gen many events with same orderId
        val orderId = "order_2203"
        val outboxes =
            (1..5).map {
                OrderPlacedEvent(
                    orderId = orderId,
                    customerId = "user_220$it",
                    items = listOf(Item(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                    totalAmountCents = 1 * it * 10 * it,
                ).toOrderOutbox(objectMapper, topicName)
            }

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))

        // publish events to kafka
        service.publish(outboxes)

        // check events landed in same topic
        val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30)).records(topicName)
        val partitionsHit = records.map { it.partition() }.toSet()
        assertThat(partitionsHit).hasSize(1)
    }

    @Test
    fun `different orderId = spread across more than 1 partition`() {
        // gen many events with different orderId
        val outboxes =
            (1..15).map {
                OrderPlacedEvent(
                    orderId = "order_220$it",
                    customerId = "user_220$it",
                    items = listOf(Item(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                    totalAmountCents = 1 * it * 10 * it,
                ).toOrderOutbox(objectMapper, topicName)
            }

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))

        // publish events to kafka
        service.publish(outboxes)

        // check events spread across more than 1 partition
        val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30)).records(topicName)
        val partitionsHit = records.map { it.partition() }.toSet()
        assertThat(partitionsHit.size).isGreaterThan(1)
    }
}
